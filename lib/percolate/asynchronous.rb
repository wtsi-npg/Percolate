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
    LSF_QUEUES = [:yesterday, :normal, :long, :basement]

    def lsf name, uid, command, log, args = {}
      defaults = {:queue     => :normal,
                  :memory    => 1900,
                  :depend    => nil,
                  :resources => nil,
                  :size      => 1}
      args = defaults.merge(args)

      queue, mem, dep, res, size =
        args[:queue], args[:memory], '', '', args[:size]

      unless LSF_QUEUES.member? queue
        raise ArgumentError, ":queue must be one of #{LSF_QUEUES.inspect}"
      end
      unless mem.is_a? Fixnum and mem > 0
        raise ArgumentError, ":memory must be a positive Fixnum"
      end
      unless size.is_a? Fixnum and size > 0
        raise ArgumentError, ":size must be a positive Fixnum"
      end

      if args[:resources]
        res = " && #{args[:resources]}"
      end
      if args[:depend]
        dep = " -w #{args[:depend]}"
      end

      jobname = "#{name}.#{uid}"
      if size > 1
        jobname << "[1-#{size}]"
      end

      "bsub -J'#{jobname}' -q #{queue} -R 'select[mem>#{mem}#{res}] " <<
              "rusage[mem=#{mem}]'#{dep} -M #{mem * 1000} " <<
              "-oo #{log} '#{command}'"
    end

    # Run or update a memoized batch command having pre- and
    # post-conditions.
    def lsf_task fname, args, command, env, procs = {}
      having, confirm, yielding = ensure_procs procs
      memos = Percolate::System.get_async_memos fname
      started, result = memos[args]

      $log.debug "Entering task #{fname}, started? #{started or 'false'}, " <<
                 "result? #{result.nil? ? 'nil' : result}"

      if started # LSF job was started
        $log.debug "#{fname} LSF job '#{command}' is already started"

        if ! result.nil?
          $log.debug "Returning memoized #{fname} result: #{result}"
        else
          begin
            if confirm.call(*args.take(confirm.arity.abs))
              yielded = yielding.call(*args.take(yielding.arity.abs))
              result = Result.new fname, yielded, []
              memos[args] = [true, result]
              $log.debug "Postconditions for #{fname} satsified; " <<
                         "returning #{result}"
            else
              $log.debug "Postconditions for #{fname} not satsified; " <<
                         "returning nil"
            end
          rescue PercolateAsyncTaskError => pate
            $log.debug "#{fname} encountered an error; #{pate.message}"
            $log.info "Resetting #{fname} for restart after error"
            memos[args] = [nil, nil]
          end
        end
      else # Can we start the LSF job?
        if ! having.call(*args.take(having.arity.abs))
          $log.debug "Preconditions for #{fname} not satisfied; " <<
                     "returning nil"
        else
          $log.debug "Preconditions for #{fname} are satisfied; " <<
                     "running '#{command}' with env #{env}"

          # Jump through hoops because bsub insists on polluting our
          # stdout
          out = []
          IO.popen command do |io|
            out = io.readlines
          end
          success = $?.exited? && $?.exitstatus.zero?
          $log.info "bsub reported #{out} for #{fname}"

          case # TODO: pass environment variables from env
            when $?.signaled?
              raise PercolateAsyncTaskError,
                    "Uncaught signal #{$?.termsig} from '#{command}'"
            when ! success
              raise PercolateAsyncTaskError,
                    "Non-zero exit #{$?.exitstatus} from '#{command}'"
            else
              memos[args] = [true, nil]
              $log.debug "#{fname} LSF job '#{command}' is running, " <<
                         "meanwhile returning nil"
          end
        end
      end

      result
    end

    def lsf_run_success? log_file
      run_success, exit_code = read_lsf_log log_file
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

      if File.exists? file
        $log.debug "Reading LSF log #{file}"

        open(file).each do |line|
          state = select_state line, state
          case state
            when :before_lsf_section, :after_lsf_section
              nil
            when :in_lsf_section
              case line
                when /^Successfully completed./
                  $log.debug "Job successfully completed in LSF log #{file}"
                  run_success = true
                  exit_code = 0
                when /^Exited with exit code (\d+)\./
                  $log.debug "Job exited with code #{$1.to_i} in LSF log" <<
                             " #{file}"
                  run_success = false
                  exit_code = $1.to_i
                when /^Exited with signal termination/
                  $log.debug "Job terminated with signal in LSF log #{file}"
                  run_success = false
              end
          end
        end
      else
        $log.debug "LSF log #{file} not created yet"
      end

      [run_success, exit_code]
    end
  end
end
