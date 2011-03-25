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
  module CommandFileIO

    def write_array_commands file, method_name, args_array, commands
      File.open(file, 'w') { |f|
        args_array.zip(commands).each { |args, cmd|
          task_id = task_identity(method_name, args)
          f.puts("#{task_id}\t#{method_name}\t#{args.inspect}\t#{cmd}")
        }
      }
    end

    def read_array_command file, lineno
      task_id = command = nil

      File.open(file, 'r') { |f|
        f.each_line { |line|
          if f.lineno == lineno
            fields = line.chomp.split("\t")
            task_id, command = fields[0], fields[3]
            break
          end
        }
      }

      if task_id.nil?
        raise PercolateError, "No such command line #{lineno} in #{file}"
      elsif task_id.empty?
        raise PercolateError, "Empty task_id at line #{lineno} in #{file}"
      elsif command.empty?
        raise PercolateError, "Empty command at line #{lineno} in #{file}"
      else
        [task_id, command]
      end
    end
  end

  class Asynchronizer
    include CommandFileIO
    attr_accessor :message_host
    attr_accessor :message_port
    attr_accessor :message_queue
    attr_accessor :async_wrapper

    def initialize args = {}
      defaults = {:message_host => 'localhost',
                  :message_port => 11300,
                  :async_wrapper => 'percolate-wrap'}
      args = defaults.merge(args)

      @message_host = args[:message_host]
      @message_port = args[:message_port]
      @async_wrapper = args[:async_wrapper]
    end

    def message_client
      Percolate.log.debug("Connecting to message host #{self.message_host} " +
                              "port #{self.message_port}")
      MessageClient.new(self.message_queue, self.message_host, self.message_port)
    end

    def async_task_aux method_name, args, command, env, callbacks = {}
      pre, post, val = ensure_callbacks(callbacks)
      memos = Percolate.memoizer.async_method_memos(method_name)
      result = memos[args]
      submitted = result && result.submitted?

      log = Percolate.log
      log.debug("Entering task #{method_name}")

      if submitted # Job was submitted
        log.debug("#{method_name} job '#{command}' is already submitted")
        update_result(method_name, args, post, val, result, log)
      else # Can we submit the job?
        if !pre.call(*args.take(pre.arity.abs))
          log.debug("Preconditions for #{method_name} not satisfied; " +
                        "returning nil")
        else
          log.debug("Preconditions for #{method_name} satisfied; " +
                        "submitting '#{command}'")

          if submit_async(method_name, command)
            task_id = task_identity(method_name, args)
            submission_time = Time.now
            memos[args] = Result.new(method_name, :async, task_id, submission_time)
          end
        end
      end

      result
    end

    protected
    def command_string task_id
      "#{self.async_wrapper} --host #{self.message_host} " +
          "--port #{self.message_port} " +
          "--queue #{self.message_queue} " +
          "--task #{task_id}"
    end

    def submit_async method_name, command
      unless self.message_queue
        raise PercolateError, "No message queue has been provided"
      end

      # TODO: check the number of open jobs versus the maximum permitted,
      # allowing submission to be throttled

      # Jump through hoops because bsub insists on polluting our stdout
      # TODO: pass environment variables from env
      status, stdout = system_command(command)
      success = command_success?(status)

      Percolate.log.info("submission reported #{stdout} for #{method_name}")

      case
        when status.signaled?
          raise PercolateAsyncTaskError,
                "Uncaught signal #{status.termsig} from '#{command}'"
        when !success
          raise PercolateAsyncTaskError,
                "Non-zero exit #{status.exitstatus} from '#{command}'"
        else
          Percolate.log.debug("#{method_name} async job '#{command}' is submitted, " +
                                  "meanwhile returning nil")
      end

      success
    end

    def update_result method_name, args, post, val, result, log, index = nil
      ix = ''
      if index
        ix = "[#{index}]"
      end

      if result.value?
        log.debug("Returning memoized #{method_name} result: #{result}")
      else
        begin
          if result.failed?
            raise PercolateAsyncTaskError,
                  "#{method_name}#{ix} args: #{args.inspect} failed"
          elsif result.finished? && post.call(*args.take(post.arity.abs))
            result.finished!(val.call(*args.take(val.arity.abs)))
            log.debug("Postconditions for #{method_name}#{ix} satsified; " +
                          "returning #{result}")
          else
            log.debug("Postconditions for #{method_name}#{ix} not satsified; " +
                          "returning nil")
          end
        rescue PercolateAsyncTaskError => pate
          # Any of the having, confirm or yielding callbacks may throw this
          log.error("#{method_name}#{ix} requires attention: #{pate.message}")
          raise pate
        end
      end

      result
    end
  end

  class SystemAsynchronizer < Asynchronizer
    include Percolate

    def async_command task_id, command, work_dir, log, args = {}
      cmd_str = command_string(task_id)
      cd(work_dir, "#{cmd_str} -- #{command} &")
    end
  end

  class LSFAsynchronizer < Asynchronizer
    include Percolate

    attr_reader :async_submitter
    attr_accessor :async_queues

    def initialize args = {}
      super(args)
      defaults = {:async_queues => [:yesterday, :small, :normal, :long, :basement],
                  :async_submitter => 'bsub'}
      args = defaults.merge(args)

      @async_queues = args[:async_queues]
      @async_submitter = args[:async_submitter]
    end

    # Wraps a command String in an LSF job submission command.
    #
    # Arguments:
    #
    # - task_id (String): a task identifier.
    # - command (String or Array): The command or Array of commands to be
    #   executed on the batch queue.
    # - work-dir (String): The working directory
    # - log (String): The path of the LSF log file to be created.
    # - args (Hash): Various arguments to LSF:
    #   - :queue     => LSF queue (Symbol) e.g. :normal, :long
    #   - :memory    => LSF memory limit in Mb (Fixnum)
    #   - :depend    => LSF job dependency (String)
    #   - :select    => LSF resource select options (String)
    #   - :reserve   => LSF resource rusage options (String)
    #
    # Returns:
    #
    # - String
    #
    def async_command task_id, command, work_dir, log, args = {}
      defaults = {:queue => :normal,
                  :memory => 1900,
                  :cpus => 1,
                  :depend => nil,
                  :select => nil,
                  :reserve => nil}
      args = defaults.merge(args)

      queue, mem, cpus = args[:queue], args[:memory], args[:cpus]
      uid = $$
      depend = select = reserve = ''

      unless self.async_queues.include?(queue)
        raise ArgumentError, ":queue must be one of #{self.async_queues.inspect}"
      end
      unless mem.is_a?(Fixnum) && mem > 0
        raise ArgumentError, ":memory must be a positive Fixnum"
      end
      unless cpus.is_a?(Fixnum) && cpus > 0
        raise ArgumentError, ":cpus must be a positive Fixnum"
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

      cpu_str = nil
      if args[:cpus] > 1
        cpu_str = " -n #{args[:cpus]} -R 'span[hosts=1]'"
      end

      cmd_str = command_string(task_id)
      job_name = "#{task_id}.#{uid}"

      if command.is_a?(Array)
        # In a job array the actual command is pulled from the job's command
        # array file using the LSF job index
        job_name << "[1-#{command.size}]"
        cmd_str << ' --index'

        unless log =~ /%I/
          raise PercolateTaskError,
                "LSF job array log '#{log}' does not countain " +
                    "a job index placeholder (%I): all jobs " +
                    "would attempt to write to the same file"
        end
      else
        # Otherwise the command is run directly
        cmd_str << " -- '#{command}'"
      end

      cd(work_dir,
         "#{self.async_submitter} -J '#{job_name}' -q #{queue} " +
             "-R 'select[mem>#{mem}#{select}] " +
             "rusage[mem=#{mem}#{reserve}]'#{depend}#{cpu_str} " +
             "-M #{mem * 1000} -oo #{log} #{cmd_str}")
    end

    def async_task_array_aux method_name, margs_arrays, commands, array_file,
        async_command, env, callbacks = {}
      pre, post, val = ensure_callbacks(callbacks)
      memos = Percolate.memoizer.async_method_memos(method_name)

      # If first in array was submitted, all were submitted
      submitted = memos.has_key?(margs_arrays.first) &&
          memos[margs_arrays.first].submitted?

      log = Percolate.log
      log.debug("Entering task #{method_name}")

      results = Array.new(margs_arrays.size)

      if submitted
        margs_arrays.each_with_index { |args, i|
          result = memos[args]
          results[i] = result
          log.debug("Checking #{method_name}[#{i}] args: #{args.inspect}, " +
                        "result: #{result}")

          update_result(method_name, args, post, val, result, log, i)
        }
      else
        # Can't submit any members of a job array until all their
        # preconditions are met
        pre = margs_arrays.collect { |args| pre.call(*args.take(pre.arity.abs)) }

        if pre.include?(false)
          log.debug("Preconditions for #{method_name} not satisfied; " +
                        "returning nil")
        else
          array_task_id = task_identity(method_name, margs_arrays)
          log.debug("Preconditions for #{method_name} are satisfied; " +
                        "submitting '#{async_command}' with env #{env}")
          log.debug("Writing #{commands.size} commands to #{array_file}")
          write_array_commands(array_file, method_name, margs_arrays, commands)

          if submit_async(method_name, async_command)
            submission_time = Time.now
            margs_arrays.each_with_index { |args, i|
              task_id = task_identity(method_name, args)
              result = Result.new(method_name, :async, task_id, submission_time)
              memos[args] = result
              log.debug("Submitted #{method_name}[#{i}] args: #{args.inspect}, " +
                            "result #{result}")
            }
          end
        end
      end

      results
    end

    private
    def count_lines file
      count = 0
      open(file).each { |line| count = count + 1 }
      count
    end
  end
end
