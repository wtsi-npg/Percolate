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

require 'percolate/system'
require 'percolate/asynchronous'
require 'percolate/tasks'
require 'percolate/workflows'
require 'percolate/percolator'
require 'percolate/partitions'

module Percolate
  $log = Logger.new STDERR

  VERSION = '0.0.6'

  # An error raised by the Percolate system.
  class PercolateError < StandardError
  end

  # An error raised by a Percolate task.
  class PercolateTaskError < PercolateError
  end

  # An error raised by an asynchronous Percolate task.
  class PercolateAsyncTaskError < PercolateTaskError
  end

  def self.cd path, command
    "cd #{path} \; #{command}"
  end

  class Result
    attr_reader 'task', 'value', 'stdout'

    def initialize task, value, stdout
      @task   = task
      @value  = value
      @stdout = stdout
    end

    def to_s
      "#<#{self.class} task: #{self.task} value: #{self.value.inspect} " <<
      "stdout: #{self.stdout}>"
    end
  end

  # Run a memoized system command having pre- and post-conditions.
  #
  # Arguments:
  #
  # - fname (Symbol): name of memoized method, unique with respect to
  #   the memoization namespace.
  # - args: (Array): arguments for system command.
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

    memos = Percolate::System.get_memos(fname)
    result = memos[args]

    $log.debug("Entering task #{fname}")

    if ! result.nil?
      $log.debug("Returning memoized result: #{result}")
      result
    elsif ! having.call(*args.take(having.arity.abs))
      $log.debug("Preconditions not satisfied, returning nil")
      nil
    else
      $log.debug("Preconditions are satisfied; running " <<
                 "'#{command}' with #{env}")

      out = []
      IO.popen(command) { |io| out = io.readlines }
      success = $?.exited? && $?.exitstatus.zero?

      case # TODO: pass environment variables from env
        when $?.signaled?
          raise PercolateTaskError,
                "Uncaught signal #{$?.termsig} from '#{command}' "
        when ! success
          raise PercolateTaskError,
                "Non-zero exit #{$?.exitstatus} from '#{command}'"
        when success && confirm.call(*args.take(confirm.arity.abs))
          yielded = yielding.call(*args.take(yielding.arity.abs))
          result = Result.new(fname, yielded, out)
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
  # - args: (Array): arguments for the Proc
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

    memos = Percolate::System.get_memos(fname)
    result = memos[args]

    $log.debug("Entering task #{fname}")

    if ! result.nil?
      $log.debug("Returning memoized result: #{result}")
      result
    elsif ! having.call(*args.take(having.arity.abs))
      $log.debug("Preconditions not satisfied, returning nil")
      nil
    else
      $log.debug("Preconditions are satisfied; calling '#{command}'")

      result = Result.new(fname, command.call(*args), nil)
      $log.debug("#{fname} called; returning #{result}")
      memos[args] = result
    end
  end

  private
  def ensure_procs procs
    [ensure_proc(:having,   procs[:having]),
     ensure_proc(:confirm,  procs[:confirm]),
     ensure_proc(:yielding, procs[:yielding])]
  end

  def ensure_proc name, proc
    if proc.is_a?(Proc)
      proc
    else
      raise ArgumentError, "a #{name} Proc is required"
    end
  end
end
