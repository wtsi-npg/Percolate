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

module Percolate
  module Asynchronous
    @@batch_submitter = 'bsub'
    @@batch_wrapper = 'percolate-wrap'
    @@batch_queues = [:yesterday, :small, :normal, :long, :basement]

    def batch_submitter
      @@batch_submitter
    end

    def batch_wrapper wrapper = nil
      if wrapper
        @@batch_wrapper = wrapper
      end
      @@batch_wrapper
    end

    def batch_queues queues = nil
      if queues
        unless queues.is_a?(Array)
          raise ArgumentError, "the queues argument must be an Array"
        end
        @@batch_queues = queues
      end
      @@batch_queues
    end

    # Wraps a command String in an LSF job submission command.
    #
    # Arguments:
    #
    # - task_id (String): a task identifier.
    # - command (String): The command to be executed on the batch queue.
    # - log (String): The path of the LSF log file to be created.
    # - args (Hash): Various arguments to LSF:
    #   - :queue     => LSF queue (Symbol) e.g. :normal, :long
    #   - :memory    => LSF memory limit in Mb (Fixnum)
    #   - :depend    => LSF job dependency (String)
    #   - :select    => LSF resource select options (String)
    #   - :reserve   => LSF resource rusage options (String)
    #   - :size      => LSF job array size (Fixnum)
    #
    # Returns:
    #
    # - String
    #
    def lsf task_id, command, work_dir, log, args = { }
      defaults = { :queue      => :normal,
                   :memory     => 1900,
                   :depend     => nil,
                   :select     => nil,
                   :reserve    => nil,
                   :array_file => nil}
      args = defaults.merge(args)

      queue, mem, depend, select, reserve, uid =
        args[:queue], args[:memory], '', '', '', $$

      unless batch_queues.include?(queue)
        raise ArgumentError, ":queue must be one of #{batch_queues.inspect}"
      end
      unless mem.is_a?(Fixnum) && mem > 0
        raise ArgumentError, ":memory must be a positive Fixnum"
      end
      if command && args[:array_file]
        raise ArgumentError,
              "Both a single command and a command array file were supplied"
      end

      if args[:select]
        select = " && #{args[:select]}"
      end
      if args[:reserve]
        reserve = ":#{args[:reserve]}"
      end
      if args[:depend]
        depend = " -w #{args[:depend]}"
      end

      cmd_str = "#{batch_wrapper} --host #{Asynchronous.message_host} " <<
                "--port #{Asynchronous.message_port} " <<
                "--queue #{Asynchronous.message_queue} " <<
                "--task #{task_id}"

      job_name = "#{task_id}.#{uid}"
      if args[:array_file]
        # In a job array the actual command is pulled from the job's
        # command array file using the LSF job index
        size = count_lines(args[:array_file])
        job_name << "[1-#{size}]"
        cmd_str << ' --index'
      else
        # Otherwise the command is run directly
        cmd_str << " -- '#{command}'"
      end

      Percolate.cd(work_dir,
                   "#{batch_submitter} -J '#{job_name}' -q #{queue} " <<
                   "-R 'select[mem>#{mem}#{select}] " <<
                   "rusage[mem=#{mem}#{reserve}]'#{depend} " <<
                   "-M #{mem * 1000} -oo #{log} #{cmd_str}")
    end

    # Run or update a memoized batch command having pre- and
    # post-conditions.
    def lsf_task fname, args, command, env, procs = { }
      having, confirm, yielding = ensure_procs(procs)
      memos = get_async_memos(fname)
      result = memos[args]
      submitted = result && result.submitted?

      $log.debug("Entering task #{fname}")

      if submitted # LSF job was submitted
        $log.debug("#{fname} LSF job '#{command}' is already submitted")

        if result.value? # if submitted, result is not nil, see above
          $log.debug("Returning memoized #{fname} result: #{result}")
        else
          begin
            if result.failed?
              raise PercolateAsyncTaskError,
                    "#{fname} args: #{args.inspect} failed"
            elsif result.finished? &&
                confirm.call(*args.take(confirm.arity.abs))
              result.finished!(yielding.call(*args.take(yielding.arity.abs)))
              $log.debug("Postconditions for #{fname} satsified; " <<
                         "returning #{result}")
            else
              $log.debug("Postconditions for #{fname} not satsified; " <<
                         "returning nil")
            end
          rescue PercolateAsyncTaskError => pate
            # Any of the having, confirm or yielding procs may throw this
            $log.error("#{fname} requires attention: #{pate.message}")
            raise pate
          end
        end
      else # Can we submit the LSF job?
        if ! having.call(*args.take(having.arity.abs))
          $log.debug("Preconditions for #{fname} not satisfied; " <<
                     "returning nil")
        else
          $log.debug("Preconditions for #{fname} satisfied; " <<
                     "submitting '#{command}'")

          if submit_async(fname, command)
            task_id = Percolate.task_identity(fname, args)
            submission_time = Time.now
            memos[args] = Result.new(fname, task_id, submission_time)
          end
        end
      end

      result
    end

    def lsf_task_array fname, args_arrays, commands, command, env, procs = { }
      having, confirm, yielding = ensure_procs(procs)
      memos = get_async_memos(fname)

      # If first in array was submitted, all were submitted
      submitted = memos.has_key?(args_arrays.first) &&
        memos[args_arrays.first].submitted?

      $log.debug("Entering task #{fname}")
      results = Array.new(args_arrays.size)

      if submitted
        args_arrays.each_with_index do |args, i|
          result = memos[args]
          results[i] = result
          $log.debug("Checking #{fname}[#{i}] args: #{args.inspect}, " <<
                     "result: #{result}")

          if result.value?
            $log.debug("Returning memoized #{fname} result: #{result}")
          else
            begin
              if result.failed?
                raise PercolateAsyncTaskError,
                     "#{fname}[#{i}] args: #{args.inspect} failed"
              elsif result.finished? &&
                  confirm.call(*args.take(confirm.arity.abs))
                result.finished!(yielding.call(*args.take(yielding.arity.abs)))
                $log.debug("Postconditions for #{fname} satsified; " <<
                           "collecting #{result}")
              else
                $log.debug("Postconditions for #{fname} not satsified; " <<
                           "collecting nil")
              end
            rescue PercolateAsyncTaskError => pate
              # Any of the having, confirm or yielding procs may throw this
              $log.error("#{fname}[#{i}] requires attention: #{pate.message}")
              raise pate
            end
          end
        end
      else
        # Can't submit any members of a job array until all their
        # preconditions are met
        pre = args_arrays.collect do |args|
          having.call(*args.take(having.arity.abs))
        end

        if pre.include?(false)
          $log.debug("Preconditions for #{fname} not satisfied; " <<
                     "returning nil")
        else
          array_task_id = Percolate.task_identity(fname, args_arrays)
          $log.debug("Preconditions for #{fname} are satisfied; " <<
                     "submitting '#{command}' with env #{env}")

          if submit_async(fname, command)
            submission_time = Time.now
            args_arrays.each_with_index do |args, i|
              task_id = Percolate.task_identity(fname, args)
              result = Result.new(fname, task_id, submission_time)
              memos[args] = result
              $log.debug("Submitted #{fname}[#{i}] args: #{args.inspect}, " <<
                         "result #{result}")
            end
          end
        end
      end

      results
    end

    def write_array_commands file, fname, args_array, commands
       File.open(file, 'w') do |f|
        args_array.zip(commands).each do |args, cmd|
          task_id = Percolate.task_identity(fname, args)
          f.puts("#{task_id}\t#{fname}\t#{args.inspect}\t#{cmd}")
        end
      end
    end

    def read_array_command file, lineno
      task_id = nil
      command = nil

      File.open(file, 'r') do |f|
        f.each_line do |line|
          if f.lineno == lineno
            fields = line.chomp.split("\t")
            task_id = fields[0]
            command = fields[3]
            break
          end
        end
      end

      if task_id.nil?
        raise PercolateError, "No such command line #{index} in #{file}"
      elsif task_id.empty?
        raise PercolateError, "Empty task_id at line #{index} in #{file}"
      elsif command.empty?
        raise PercolateError, "Empty command at line #{index} in #{file}"
      else
        [task_id, command]
      end
    end

    def count_lines file
      count = 0
      open(file).each { |line| count = count + 1 }
      count
    end

    def submit_async fname, command
      # Check that the message queue has been set
      unless Asynchronous.message_queue
        raise PercolateError, "No message queue has been provided"
      end

      # Jump through hoops because bsub insists on polluting our
      # stdout
      # TODO: pass environment variables from env
      status, stdout = system_command(command)
      success = command_success?(status)

      $log.info("submission reported #{stdout} for #{fname}")

      case
        when status.signaled?
          raise PercolateAsyncTaskError,
                "Uncaught signal #{status.termsig} from '#{command}'"
        when ! success
          raise PercolateAsyncTaskError,
                "Non-zero exit #{status.exitstatus} from '#{command}'"
      else
        $log.debug("#{fname} async job '#{command}' is submitted, " <<
                   "meanwhile returning nil")
      end

      success
    end
  end
end
