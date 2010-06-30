#--
#
# Copyright (C) 2010 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

require 'fileutils'
require 'logger'

require 'percolate/memoize'
require 'percolate/message_queue'
require 'percolate/asynchronous'
require 'percolate/tasks'
require 'percolate/workflows'
require 'percolate/percolator'
require 'percolate/partitions'

module Percolate
  include Percolate::Memoize
  $log = Logger.new(STDERR)

  VERSION = '0.1.0'

  # An error raised by the Percolate system.
  class PercolateError < StandardError
  end

  # An error raised by a Percolate task.
  class PercolateTaskError < PercolateError
  end

  # An error raised by an asynchronous Percolate task.
  class PercolateAsyncTaskError < PercolateTaskError
  end

  # Returns a task identity string for a call to function named fname
  # with arguments Array args.
  def self.task_identity fname, args
    fname.to_s + '-' + Digest::MD5.hexdigest(fname.to_s + args.inspect)
  end

  # Returns a copy of String command with a change directory operation
  # prepended.
  def self.cd path, command
    "cd #{path} \; #{command}"
  end

  # A result of running an external program, including metadata (time
  # started and finished, exit code).
  #
  class Result
    # The name of task responsible for the result
    attr_reader   :task
    # The unique identity of the task instance responsible for the
    # result
    attr_reader   :task_identity

    # The submission time, if available
    attr_accessor :submission_time
    # The start time, if available
    attr_accessor :start_time
    # The finish time, if available
    attr_accessor :finish_time

    # Task return value
    attr_accessor :value
    # Task exit code
    attr_accessor :exit_code
    # Task SDTOUT
    attr_accessor :stdout
    # Task STDERR
    attr_accessor :stderr

    # Boolean true if submitted
    attr_accessor :submitted
    # Boolean true if started
    attr_accessor :started
    # Boolean true if finished
    attr_accessor :finished
    protected     :submitted, :started, :finished

    def initialize task, task_identity, submission_time, start_time = nil,
                   finish_time = nil, value = nil, stdout = nil, stderr = nil
      @task            = task
      @task_identity   = task_identity
      @submission_time = submission_time
      @start_time      = start_time
      @finish_time     = finish_time
      @value           = value
      @stdout          = stdout
      @stderr          = stderr
    end

    # Sets the Result on completion of a task.
    def finished! value, finish_time = Time.now, exit_code = 0
      self.finish_time = finish_time
      self.exit_code = exit_code
      self.value = value
    end

    # Sets the time at which the task started. Tasks may be restarted,
    # in which case the finish time, value, stdout and stderr are set
    # to nil
    def started! start_time = Time.now
      self.start_time = start_time
      self.finish_time = nil
      self.value = nil
      self.stdout = nil
      self.stderr = nil
    end

    # Returns true if the task that will generate the Result's value
    # has been submitted.
    def submitted?
      ! self.submission_time.nil?
    end

    # Returns true if the task that will generate the Result's value
    # has been started.
    def started?
      ! self.start_time.nil?
    end

    # Returns true if the task that will generate the Result's value
    # has finished.
    def finished?
      ! self.finish_time.nil?
    end

    # Return true if the task that will generate the Result's value
    # has returned something i.e. the value is not nil.
    def value?
      ! self.value.nil?
    end

    def failed?
      self.finished? && ! self.exit_code.zero?
    end

    def to_s
      "#<#{self.class} task_id: #{self.task_identity} " <<
      "sub: #{self.submission_time.inspect} " <<
      "start: #{self.start_time.inspect} " <<
      "finish: #{self.finish_time.inspect} value: #{self.value.inspect}>"
    end
  end

  # Run a memoized system command having pre- and post-conditions.
  #
  # Arguments:
  #
  # - fname (Symbol): name of memoized method, unique with respect to
  #   the memoization namespace.
  # - args: (Array): memoization key arguments.
  # - command (String): system command string.
  # - env (Hash): hash of shell environment variable Strings for the
  #   system command.
  #
  # - procs (Hash): hash of named Procs
  #
  #   - :having => pre-condition Proc, should evaluate true if
  #     pre-conditions of execution are satisfied
  #   - :confirm => post-condition Proc, should evaluate true if
  #     post-conditions of execution are satisfied
  #   - :yielding => return value Proc, should evaluate to the desired
  #     return value
  #
  #  These Procs may accept no, some, or all the arguments that are
  #  passed to the system command. Each will be called with the
  #  appropriate number. For example, if the :having Proc has arity 2,
  #  it will be called with the first 2 elements of args.
  #
  # Returns:
  # - Return value of the :yielding Proc, or nil.
  def task fname, args, command, env, procs = {}
    having, confirm, yielding = ensure_procs(procs)

    memos = get_memos(fname)
    result = memos[args]

    $log.debug("Entering task #{fname}")

    if result && result.value?
      $log.debug("Returning memoized result: #{result}")
      result
    elsif ! having.call(*args.take(having.arity.abs))
      $log.debug("Preconditions not satisfied, returning nil")
      nil
    else
      $log.debug("Preconditions satisfied; running '#{command}'")

      submission_time = start_time = Time.now
      status, stdout = system_command(command)
      finish_time = Time.now
      success = command_success?(status)

      case # TODO: pass environment variables from env
        when status.signaled?
          raise PercolateTaskError,
                "Uncaught signal #{status.termsig} from '#{command}'"
        when ! success
          raise PercolateTaskError,
                "Non-zero exit #{status.exitstatus} from '#{command}'"
        when success && confirm.call(*args.take(confirm.arity.abs))
          yielded = yielding.call(*args.take(yielding.arity.abs))
          task_id = Percolate.task_identity(fname, args)
          result = Result.new(fname, task_id, submission_time, start_time,
                              finish_time, yielded, status.exitstatus, stdout)
          $log.debug("Postconditions satsified; returning #{result}")
          memos[args] = result
        else
          $log.debug("Postconditions not satsified; returning nil")
          nil
      end
    end
  end

  # Run a memoized Ruby Proc having pre-conditions
  #
  # Arguments:
  #
  # - fname (Symbol): name of memoized method, unique with respect to
  #   the memoization namespace.
  # - args: (Array): memoization key arguments.
  # - command (Proc): the Proc to memoize
  # - having: (Proc):  pre-condition Proc, should evaluate true if
  #   pre-conditions of execution are satisfied
  #
  #  The 'having' Proc may accept no, some, or all the arguments that
  #  are passed to the 'command' Proc. It will be called with the
  #  appropriate number. For example, if the 'having' Proc has arity 2,
  #  it will be called with the first 2 elements of args.
  #
  # Returns:
  # - Return value of the :command Proc, or nil.
  def native_task fname, args, command, having
    ensure_proc('command', command)
    ensure_proc('having', having)

    memos = get_memos(fname)
    result = memos[args]

    $log.debug("Entering task #{fname}")

    if result
      $log.debug("Returning memoized result: #{result}")
      result
    elsif ! having.call(*args.take(having.arity.abs))
      $log.debug("Preconditions not satisfied, returning nil")
      nil
    else
      $log.debug("Preconditions are satisfied; calling '#{command}'")

      submission_time = start_time = Time.now
      task_id = Percolate.task_identity(fname, args)
      value = command.call(*args)
      finish_time = Time.now

      result = Result.new(fname, task_id, submission_time, start_time,
                         finish_time, value, nil, nil)
      $log.debug("#{fname} called; returning #{result}")
      memos[args] = result
    end
  end

  private
    def ensure_procs procs
    [:having, :confirm, :yielding].collect { |k| ensure_proc(k, procs[k]) }
  end

  def ensure_proc key, proc
    if proc.is_a?(Proc)
      proc
    else
      raise ArgumentError, "a #{key} Proc is required"
    end
  end

  def system_command command
    out = []
    IO.popen(command) { |io| out = io.readlines }
    [$?, out]
  end

  def command_success? process_status
    process_status.exited? && process_status.exitstatus.zero?
  end
end
