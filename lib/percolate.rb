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

module Percolate
  $log = Logger.new STDERR

  VERSION = '0.0.1'

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
  def task fname, args, command, env, procs = {}
    having, confirm, yielding = ensure_procs procs

    memos = Percolate::System.get_memos fname
    result = memos[args]

    $log.debug "Entering task #{fname}"

    if ! result.nil?
      $log.debug "Returning memoized result: #{result}"
      result
    elsif ! having.call(*args.take(having.arity.abs))
      $log.debug "Preconditions not satisfied, returning nil"
      nil
    else
      $log.debug "Preconditions are satisfied; running '#{command}' with #{env}"

      out = []
      IO.popen command do |io|
        out = io.readlines
      end
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
          result = Result.new fname, yielded, out
          $log.debug "Postconditions satsified; returning #{result}"
          memos[args] = result
        else
          $log.debug "Postconditions not satsified; returning nil"
          nil
      end
    end
  end

  def ensure_procs procs # :nodoc
    [ensure_proc(:having,   procs[:having]),
     ensure_proc(:confirm,  procs[:confirm]),
     ensure_proc(:yielding, procs[:yielding])]
  end

  def ensure_proc name, proc # :nodoc
    if proc.is_a? Proc
      proc
    else
      raise ArgumentError, "a #{name} Proc is required"
    end
  end
end
