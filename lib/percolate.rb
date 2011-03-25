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

require 'digest'
require 'fileutils'
require 'logger'
require 'uri'

require 'percolate/memoizer'
require 'percolate/message_client'
require 'percolate/asynchronizer'
require 'percolate/tasks'
require 'percolate/workflows'
require 'percolate/percolator'
require 'percolate/partitions'

module Percolate

  VERSION = '0.4.1'

  @log = Logger.new(STDERR)
  @memoizer = Percolate::Memoizer.new
  @asynchronizer = Percolate::LSFAsynchronizer.new

  # An error raised by the Percolate system.
  class PercolateError < StandardError
  end

  # An error raised by a Percolate task.
  class PercolateTaskError < PercolateError
  end

  # An error raised by an asynchronous Percolate task.
  class PercolateAsyncTaskError < PercolateTaskError
  end

  class << self
    attr_accessor :log
    attr_accessor :memoizer
    attr_accessor :asynchronizer
  end

  # A result of running an external program, including metadata (time
  # started and finished, exit code).
  #
  class Result
    COLUMN_NAMES = [:task, :mode, :task_identity,
                    :submission_time, :start_time, :finish_time, :run_time,
                    :exit_code]

    # The name of task responsible for the result
    attr_reader :task
    # The task mode e.g. :native, :sync or :async
    attr_reader :mode
    # The unique identity of the task instance responsible for the
    # result
    attr_reader :task_identity

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

    def initialize task, mode, task_identity, submission_time, start_time = nil,
        finish_time = nil, value = nil, stdout = nil, stderr = nil
      @task = task
      @mode = mode
      @task_identity = task_identity
      @submission_time = submission_time
      @start_time = start_time
      @finish_time = finish_time
      @value = value
      @stdout = stdout
      @stderr = stderr
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
      !self.submission_time.nil?
    end

    # Returns true if the task that will generate the Result's value
    # has been started.
    def started?
      !self.start_time.nil?
    end

    # Returns true if the task that will generate the Result's value
    # has finished.
    def finished?
      !self.finish_time.nil?
    end

    # Return true if the task that will generate the Result's value
    # has returned something i.e. the value is not nil.
    def value?
      !self.value.nil?
    end

    def failed?
      self.finished? && !self.exit_code.zero?
    end

    def run_time
      if started? && finished?
        self.finish_time - self.start_time
      end
    end

    def to_a
      [self.task, self.mode, self.task_identity,
       self.submission_time, self.start_time, self.finish_time, self.run_time,
       self.exit_code].collect { |x| x.nil? ? 'NA' : x }
    end

    def to_s
      vstr = self.value.inspect
      if vstr.length > 124
        vstr = vstr[0, 124] + " ..."
      end

      "#<#{self.class} #{self.mode} task_id: #{self.task_identity} " +
      "sub: #{self.submission_time.inspect} " +
      "start: #{self.start_time.inspect} " +
      "finish: #{self.finish_time.inspect} value: #{vstr}>"
    end
  end

  # Returns a task identity string for a call to method named method_name
  # with arguments Array args.
  def task_identity method_name, args
    Digest::MD5.hexdigest(method_name.to_s + args.inspect) + '-' + method_name.to_s
  end

  # Returns a copy of String command with a change directory operation
  # prepended.
  def cd path, command
    "cd #{path} \; #{command}"
  end

  def task margs, command, args = {}
    unwrap = args.delete(:unwrap)
    mname = calling_method
    env = {}

    result = task_aux(mname, margs, command, env, callback_defaults.merge(args))
    maybe_unwrap(result, unwrap)
  end

  def native_task margs, command, pre = lambda { true }, args = {}
    mname = calling_method
    unwrap = args.delete(:unwrap)

    result = native_task_aux(mname, margs, command, pre)
    maybe_unwrap(result, unwrap)
  end

  def async_task margs, command, work_dir, log, args = {}
    unwrap = args.delete(:unwrap)
    async = args.delete(:async) || {}
    mname = calling_method
    env = {}

    callbacks, other = split_task_args(args)
    task_id = task_identity(mname, margs)
    async_command = async_command(task_id, command, work_dir, log, async)

    result = Percolate.asynchronizer.async_task_aux(mname, margs,
                                                    async_command, env,
                                                    callback_defaults.merge(callbacks))
    maybe_unwrap(result, unwrap)
  end

  def async_task_array margs_arrays, commands, work_dir, log, args = {}
    unwrap = args.delete(:unwrap)
    async = args.delete(:async) || {}
    mname = calling_method
    env = {}

    callbacks, other = split_task_args(args)
    task_id = task_identity(mname, margs_arrays)
    array_file = File.join(work_dir, "#{task_id}.txt")
    async_command = async_command(task_id, commands, work_dir, log, async)

    result = Percolate.asynchronizer.async_task_array_aux(mname, margs_arrays,
                                                          commands, array_file,
                                                          async_command, env,
                                                          callback_defaults.merge(callbacks))
    maybe_unwrap(result, unwrap)
  end

  def async_command *args
    Percolate.asynchronizer.async_command(*args)
  end

  private
  # Run a memoized system command having pre- and post-conditions.
  #
  # Arguments:
  #
  # - method_name (Symbol): name of memoized method, unique with respect to
  #   the memoization namespace.
  # - args: (Array): memoization key arguments.
  # - command (String): system command string.
  # - env (Hash): hash of shell environment variable Strings for the
  #   system command.
  #
  # - callbacks (Hash): hash of named Procs
  #
  #   - :pre => pre-condition Proc, should evaluate true if
  #     pre-conditions of execution are satisfied
  #   - :post => post-condition Proc, should evaluate true if
  #     post-conditions of execution are satisfied
  #   - :result => return value Proc, should evaluate to the desired
  #     return value
  #
  #  These Procs may accept no, some, or all the arguments that are
  #  passed to the system command. Each will be called with the
  #  appropriate number. For example, if the :pre Proc has arity 2,
  #  it will be called with the first 2 elements of args.
  #
  # Returns:
  # - Return value of the :result Proc, or nil.
  def task_aux method_name, margs, command, env, args = {}
    pre, post, val = ensure_callbacks(args)

    memos = Percolate.memoizer.method_memos(method_name)
    result = memos[margs]
    log = Percolate.log
    log.debug("Entering task #{method_name}")

    if result && result.value?
      log.debug("Returning memoized result: #{result}")
      result
    elsif !pre.call(*margs.take(pre.arity.abs))
      log.debug("Preconditions not satisfied, returning nil")
      nil
    else
      log.debug("Preconditions satisfied; running '#{command}'")

      submission_time = start_time = Time.now
      status, stdout = system_command(command)
      finish_time = Time.now
      success = command_success?(status)

      case # TODO: pass environment variables from env
        when status.signaled?
          raise PercolateTaskError,
                "Uncaught signal #{status.termsig} from '#{command}'"
        when !success
          raise PercolateTaskError,
                "Non-zero exit #{status.exitstatus} from '#{command}'"
        when success && post.call(*margs.take(post.arity.abs))
          value = val.call(*margs.take(val.arity.abs))
          task_id = task_identity(method_name, margs)
          result = Result.new(method_name, :sync, task_id,
                              submission_time, start_time, finish_time,
                              value, status.exitstatus, stdout)
          log.debug("Postconditions satsified; returning #{result}")
          memos[margs] = result
        else
          log.debug("Postconditions not satsified; returning nil")
          nil
      end
    end
  end

  # Run a memoized Ruby Proc having pre-conditions
  #
  # Arguments:
  #
  # - method_name (Symbol): name of memoized method, unique with respect to
  #   the memoization namespace.
  # - args: (Array): memoization key arguments.
  # - command (Proc): the Proc to memoize
  # - pre: (Proc):  pre-condition Proc, should evaluate true if
  #   pre-conditions of execution are satisfied
  #
  #  The 'pre' Proc may accept no, some, or all the arguments that
  #  are passed to the 'command' Proc. It will be called with the
  #  appropriate number. For example, if the 'pre' Proc has arity 2,
  #  it will be called with the first 2 elements of args.
  #
  # Returns:
  # - Return value of the :command Proc, or nil.
  def native_task_aux method_name, margs, command, pre
    ensure_callback('command', command)
    ensure_callback('pre', pre)

    memos = Percolate.memoizer.method_memos(method_name)
    result = memos[margs]
    log = Percolate.log
    log.debug("Entering task #{method_name}")

    if result
      log.debug("Returning memoized result: #{result}")
      result
    elsif !pre.call(*margs.take(pre.arity.abs))
      log.debug("Preconditions not satisfied, returning nil")
      nil
    else
      log.debug("Preconditions are satisfied; calling '#{command}'")

      submission_time = start_time = Time.now
      task_id = task_identity(method_name, margs)
      value = command.call(*margs)
      finish_time = Time.now

      result = Result.new(method_name, :native, task_id,
                          submission_time, start_time, finish_time,
                          value, nil, nil)
      log.debug("#{method_name} called; returning #{result}")
      memos[margs] = result
    end
  end

  def calling_method
    if caller[1] =~ /`([^']*)'/
      $1.to_sym
    else
      raise PercolateError,
            "Failed to determine Percolate method name from '#{caller[0]}'"
    end
  end

  def split_task_args args
    callbacks = args.reject {|key, value| ! [:pre, :post, :result].include?(key) }
    other = args.reject {|key, value| callbacks.keys.include?(key) }

    [callbacks, other]
  end

  def maybe_unwrap result, unwrap
    if result && unwrap != false
      result.value
    else
      result
    end
  end

  def ensure_callbacks callbacks
    [:pre, :post, :result].collect { |k| ensure_callback(k, callbacks[k]) }
  end

  def ensure_callback key, callback
    if callback.is_a?(Proc)
      callback
    else
      raise ArgumentError, "a #{key} Proc is required"
    end
  end

  def callback_defaults
    {:pre => lambda { true },
     :post => lambda { true }}
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
