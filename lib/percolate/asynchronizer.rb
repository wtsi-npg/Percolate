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
  module CommandFileIO
    # Writes a tab-delimited file containing the commands to be run in a
    # batch job array, one per line. Each line contains: task_id, method_name,
    # argument list and command, separated by tab characters.
    #
    # Arguments:
    # - file (String): The file to write.
    # - method_name (Symbol): The name of the Percolate method that wraps the
    #   command.
    # - margs_arraym (Array of Arrays): The arguments for each wrapper method
    #   call. Each element is an argument Array of a separate call.
    # - commands (Array of Strings): The commands to be executed. This and the
    #   args_array must be the same length.
    #
    # Returns:
    #
    # file (String)
    def write_array_commands(file, method_name, margs_arrays, commands)
      File.open(file, 'w') do |f|
        margs_arrays.zip(commands).each do |margs, cmd|
          task_id = task_identity(method_name, *margs)
          f.puts("#{task_id}\t#{method_name}\t#{margs.inspect}\t#{cmd}")
        end
      end
      file
    end

    # Reads a single command line from a tab-delimited file containing the
    # commands to be run in a batch job array, one per line.
    #
    # Arguments:
    #
    # - file (String): The file to read.
    # - lineno (integer): The line to read (as counted by IO.each_line)
    #
    # Returns:
    #
    # - Array of String, task id and command.
    def read_array_command(file, lineno)
      task_id = command = nil

      File.open(file, 'r') do |f|
        f.each_line do |line|
          if f.lineno == lineno
            fields = line.chomp.split("\t")
            task_id, command = fields[0], fields[3]
            break
          end
        end
      end

      case
        when task_id.nil?
          raise PercolateError, "No such command line #{lineno} in #{file}"
        when task_id.empty?
          raise PercolateError, "Empty task_id at line #{lineno} in #{file}"
        when command.empty?
          raise PercolateError, "Empty command at line #{lineno} in #{file}"
        else
          [task_id, command]
      end
    end
  end

  # An Asynchronizer is responsible for starting tasks that are run
  # asynchronously as system calls or on batch queues, watching their
  # state as they run and recording any changes.
  #
  # Once launched, a task will call back to a message queue which the
  # Asynchronizer watches. The Asynchronizer uses this information and a
  # Memoizer to determine what should be done with each subsequent task
  # method invocation.
  module Asynchronizer
    include Percolate
    include Tasks
    include CommandFileIO

    # Method for executing an asynchronous task.
    def async_task(method_name, margs, command, env, callbacks = {})
      pre, post, val = ensure_callbacks(callbacks)
      memoizer = Percolate.memoizer
      memos = memoizer.async_method_memos(method_name)
      result = memos[margs]
      submitted = result && result.submitted?

      log = Percolate.log
      log.debug("Entering task #{method_name}")

      if submitted # Job was submitted
        log.debug("#{method_name} job '#{command}' is already submitted")
        update_result(method_name, margs, post, val, result, log)
      else # Can we submit the job?
        if !memoizer.free_async_slots?
          log.debug("Deferring submission of #{method_name}; " +
                        "returning nil")
        elsif !pre.call(*margs.take(pre.arity.abs))
          log.debug("Preconditions for #{method_name} not satisfied; " +
                        "returning nil")
        else
          log.debug("Preconditions for #{method_name} satisfied; " +
                        "submitting '#{command}'")

          if submit_async(method_name, command)
            task_id = task_identity(method_name, *margs)
            submission_time = Time.now
            memos[margs] = Result.new(method_name, :async, task_id, submission_time)
          end
        end
      end

      result
    end

    protected
    # Makes a system call for a named asynchronous method. The system call
    # executes a command that initiates work in a separate process and then
    # returns.
    def submit_async(method_name, command)
      unless self.message_queue
        raise PercolateError, "No message queue has been provided"
      end

      # Jump through hoops because bsub insists on polluting our stdout
      # TODO: pass environment variables from env
      status, stdout = system_command(command)
      success = command_success?(status)

      Percolate.log.info("submission reported #{stdout} for #{method_name}")

      case
        when status.signaled?
          raise AsyncTaskError,
                "Uncaught signal #{status.termsig} from '#{command}'"
        when !success
          raise AsyncTaskError,
                "Non-zero exit #{status.exitstatus} from '#{command}'"
        else
          Percolate.log.debug("#{method_name} async job '#{command}' is submitted, " +
                                  "meanwhile returning nil")
      end

      success
    end

    # Updates a pending result for a named asynchronous method.
    def update_result(method_name, args, post, val, result, log, index = nil)
      ix = index ? "[#{index}]" : ''

      if result.value?
        log.debug("Returning memoized #{method_name} result: #{result}")
      else
        begin
          case
            when result.failed?
              raise AsyncTaskError,
                    "#{method_name}#{ix} args: #{args.inspect} failed"
            when result.finished? && post.call(*args.take(post.arity.abs))
              result.finished!(val.call(*args.take(val.arity.abs)))
              log.debug("Postconditions for #{method_name}#{ix} satsified; " +
                            "returning #{result}")
            else
              log.debug("Postconditions for #{method_name}#{ix} not satsified; " +
                            "returning nil")
          end
        rescue AsyncTaskError => pate
          # Any of the having, confirm or yielding callbacks may throw this
          log.error("#{method_name}#{ix} requires attention: #{pate.message}")
          raise pate
        end
      end

      result
    end
  end
end
