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
    #   - :resources => LSF resource requirements (String)
    #   - :size      => LSF job array size (Fixnum)
    #
    # Returns:
    #
    # - String
    #
    def lsf task_id, command, work_dir, log, args = {}
      defaults = {:queue     => :normal,
                  :memory    => 1900,
                  :depend    => nil,
                  :resources => nil,
                  :size      => 1}
      args = defaults.merge(args)

      queue, mem, dep, res, size =
        args[:queue], args[:memory], '', '', args[:size]

      unless batch_queues.member?(queue)
        raise ArgumentError, ":queue must be one of #{batch_queues.inspect}"
      end
      unless mem.is_a?(Fixnum) && mem > 0
        raise ArgumentError, ":memory must be a positive Fixnum"
      end
      unless size.is_a?(Fixnum) && size > 0
        raise ArgumentError, ":size must be a positive Fixnum"
      end

      if args[:resources]
        res = " && #{args[:resources]}"
      end
      if args[:depend]
        dep = " -w #{args[:depend]}"
      end

      uid = $$
      jobname = "#{task_id}.#{uid}"
      if size > 1
        jobname << "[1-#{size}]"
      end

      cmd_str = "#{batch_wrapper} --host #{Asynchronous.message_host} " <<
                "--port #{Asynchronous.message_port} " <<
                "--queue #{Asynchronous.message_queue} " <<
                "--task #{task_id} -- #{command}"

      Percolate.cd(work_dir,
                   "#{batch_submitter} -J'#{jobname}' -q #{queue} " <<
                   "-R 'select[mem>#{mem}#{res}] " <<
                   "rusage[mem=#{mem}]'#{dep} " <<
                   "-M #{mem * 1000} -oo #{log} '#{cmd_str}'")
    end

    # Run or update a memoized batch command having pre- and
    # post-conditions.
    def lsf_task fname, args, command, env, procs = {}
      having, confirm, yielding = ensure_procs(procs)
      memos = get_async_memos(fname)
      result = memos[args]
      submitted = result && result.submitted?

      task_id = Percolate.task_identity(fname, args)
      $log.debug("Entering task #{fname}")

      if submitted # LSF job was submitted
        $log.debug("#{fname} LSF job '#{command}' is already submitted")

        if result.value? # if submitted, result is not nil, see above
          $log.debug("Returning memoized #{fname} result: #{result}")
        else
          begin
            if result.finished? && confirm.call(*args.take(confirm.arity.abs))
              result.finished!(yielding.call(*args.take(yielding.arity.abs)))
              $log.debug("Postconditions for #{fname} satsified; " <<
                         "returning #{result}")
            else
              $log.debug("Postconditions for #{fname} not satsified; " <<
                         "returning nil")
            end
          rescue PercolateAsyncTaskError => pate
            $log.debug("#{fname} encountered an error; #{pate.message}")
            $log.info("Resetting #{fname} for resubmission after error")
            memos.delete(args)
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

    def lsf_task_array fname, args_arrays, command, env, logs, procs = {}
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

          if result && result.value?
            $log.debug "Collecting memoized #{fname} result: #{result}"
            results[i] = result
          else
            begin
              if result.finished? && confirm.call(*args.take(confirm.arity.abs))
                value = yielding.call(*args.take(yielding.arity.abs))
                result.finished!(value)
                results[i] = result # FIXME -- unecessary?
                $log.debug("Postconditions for #{fname} satsified; " <<
                           "collecting #{result}")
              else
                $log.debug("Postconditions for #{fname} not satsified; " <<
                           "collecting nil")
              end
            rescue PercolateAsyncTaskError => pate
              $log.debug("#{fname} encountered an error; #{pate.message}")
              $log.info("Resetting #{fname} for resubmission after error")
              # How do you resubmit a subset of a job array?
              # memos.delete(args) FIXME -- must requeue these instead
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
          $log.debug("Preconditions for #{fname} are satisfied; " <<
                     "submitting '#{command}' with env #{env}")

          if submit_async(fname, command)
            submission_time = Time.now
            args_arrays.each do |args|
              task_id = Percolate.task_identity(fname, args)
              memos[args] = Result.new(fname, task_id, submission_time)
            end
          end
        end
      end

      results
    end

    def submit_async fname, command
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

    def lsf_run_success? log_file
      run_success, exit_code = read_lsf_log(log_file)
      if run_success == false
        raise PercolateAsyncTaskError,
              "Task failed with exit code #{exit_code}"
      end

      run_success
    end

    def read_lsf_log file
      def select_state line, current_state
        case line
          when NilClass
            current_state
          when /^Your job looked like:/
            :in_lsf_section
          when /^The output (if any) is above this job summary."/
            :after_lsf_section
          else
            current_state
        end
      end

      state = :before_lsf_section
      run_success = nil
      exit_code = nil

      if File.exists?(file)
        $log.debug("Reading LSF log #{file}")

        open(file).each do |line|
          state = select_state(line, state)
          case state
            when :before_lsf_section, :after_lsf_section
              nil
            when :in_lsf_section
              case line
                when /^Successfully completed./
                  $log.debug("Job successfully completed in LSF log #{file}")
                  run_success = true
                  exit_code = 0
                when /^Exited with exit code (\d+)\./
                  $log.debug("Job exited with code #{$1.to_i} in LSF log" <<
                             " #{file}")
                  run_success = false
                  exit_code = $1.to_i
                when /^Exited with signal termination/
                  $log.debug("Job terminated with signal in LSF log #{file}")
                  run_success = false
              end
          end
        end
      else
        $log.debug("LSF log #{file} not created yet")
      end

      [run_success, exit_code]
    end
  end
end
