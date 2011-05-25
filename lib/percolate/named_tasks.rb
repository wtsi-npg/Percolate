#--
#
# Copyright (c) 2010-2011 Genome Research Ltd. All rights reserved.
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

module Percolate
  module NamedTasks
    # Returns a task identity string for a call to method named method_name
    # with arguments Array args. Optionally a custom serializer for args may
    # be provided.
    def task_identity(method_name, args, &serializer)

      # Maybe use respond_to? to check for available serialization or checksumming
      # libraries? Using inspect is a bit rubbish.
      serial = if serializer
                 serializer.call(args)
               else
                 args.inspect
               end

      Digest::MD5.hexdigest(method_name.to_s + serial) + '-' + method_name.to_s
    end

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
    # Other arguments (keys and values):
    #
    # - :pre (Proc): A precondition callback that must evaluate true before
    #   the task is executed. Optional.
    # - :post (Proc): A postcondition callback that must evaluate true before
    #   a result is returned. Optional.
    # - :result (Proc): A return value callback that must evaluate to the desired
    #   return value.
    #
    # Returns:
    # - Wrapped return value of the :result Proc, or nil.
    def task(method_name, margs, command, env, args = {})
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
    # - method_name (Symbol): Name of memoized method, unique with respect to
    #   the memoization namespace.
    # - margs (Array): Memoization key arguments.
    # - pre (Proc): Pre-condition Proc, should evaluate true if
    #   pre-conditions of execution are satisfied.
    # - proc (Proc): The Proc to memoize.
    #
    # Returns:
    # - Wrapped return value of the :result Proc, or nil.
    def native_task(method_name, margs, pre, proc)
      ensure_callback('pre', pre)
      ensure_callback('result', proc)

      memos = Percolate.memoizer.method_memos(method_name)
      result = memos[margs]
      log = Percolate.log
      log.debug("Entering task #{method_name}")

      case
        when result
          log.debug("Returning memoized result: #{result}")
          result
        when !pre.call(*margs.take(pre.arity.abs))
          log.debug("Preconditions not satisfied, returning nil")
          nil
        else
          log.debug("Preconditions are satisfied; calling '#{proc.inspect}'")

          submission_time = start_time = Time.now
          task_id = task_identity(method_name, margs)
          value = proc.call(*margs)
          finish_time = Time.now

          result = Result.new(method_name, :native, task_id,
                              submission_time, start_time, finish_time,
                              value, nil, nil)
          log.debug("#{method_name} called; returning #{result}")
          memos[margs] = result
      end
    end

    protected
    def ensure_callbacks(callbacks)
      [:pre, :post, :result].collect { |k| ensure_callback(k, callbacks[k]) }
    end

    def ensure_callback(key, callback)
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
  end
end
