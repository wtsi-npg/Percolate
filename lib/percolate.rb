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

  ## An error raised by the Percolate system.
  class PercolateError < StandardError
  end

  ## An error raised by a Percolate task.
  class PercolateTaskError < PercolateError
  end

  def cd path, command
    "cd #{path} \; #{command}"
  end

  ## Run a memoized system command having pre- and post-conditions.
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

      case system(command) # TODO: pass environment variables from env
        when NilClass
          raise PercolateTaskError, "Unexpected error executing '#{command}'"
        when FalseClass
          raise PercolateTaskError, "Non-zero exit #{$?} from '#{command}'"
        when confirm.call(*args.take(confirm.arity.abs))
          result = yielding.call(*args.take(yielding.arity.abs))
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
