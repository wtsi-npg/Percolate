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
  # An Asynchronizer that submits jobs to platform LSF batch queues.
  class LSFAsynchronizer < TaskWrapper
    include Percolate
    include Utilities
    include Asynchronizer

    attr_reader :async_submitter
    attr_accessor :async_queues
    attr_reader :data_registrar

    def initialize(args = {})
      super(args)
      defaults = {:async_queues => [:yesterday, :small, :normal, :long, :basement],
                  :async_submitter => 'bsub', :data_registrar => 'datactrl'}
      args = defaults.merge(args)

      @async_queues = args[:async_queues]
      @async_submitter = args[:async_submitter]
      @data_registrar = args[:data_registrar]
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
    def async_command(task_id, command, work_dir, log, args = {})
      defaults = {:queue => :normal,
                  :memory => 1900,
                  :cpus => 1,
                  :depend => nil,
                  :select => nil,
                  :reserve => nil,
                  :storage => {},
                  :dataset => nil}
      args = defaults.merge(args)

      queue, mem, cpus = args[:queue], args[:memory], args[:cpus]
      uid = $$
      depend = select = reserve = ''
      storage, dataset = args[:storage], args[:dataset]
      sdistance = ssize = nil

      unless self.async_queues.include?(queue)
        raise ArgumentError, ":queue must be one of #{self.async_queues.inspect}"
      end
      unless mem.is_a?(Fixnum) && mem > 0
        raise ArgumentError, ":memory must be a positive Fixnum"
      end
      unless cpus.is_a?(Fixnum) && cpus > 0
        raise ArgumentError, ":cpus must be a positive Fixnum"
      end
      if !storage.empty?
        if dataset
          raise ArgumentError, ":storage and :dataset must not be provided together"
        end
        ssize = storage[:size]
        sdistance = storage[:distance]
        unless ssize && ssize.is_a?(Fixnum) && ssize > 0
          raise ArgumentError, ":storage SIZE must be a positive Fixnum"
        end
        unless sdistance && sdistance.is_a?(Fixnum) && sdistance >= 0
          raise ArgumentError,
                ":storage DISTANCE must be a non-negative Fixnum, but was '#{sdistance}'"
        end
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
      if cpus > 1
        cpu_str = " -n #{cpus} -R 'span[hosts=1]'"
      end

      cmd_str = command_string(task_id)
      job_name = "#{task_id}.#{uid}"

      extsched_str = ''
      if sdistance && ssize
        cmd_str << ' --storage'
        extsched_str = " -extsched 'storage[size=#{ssize};distance=#{sdistance}]' "
      elsif dataset
        extsched_str = " -extsched 'dataset[name=#{dataset}]' "
      end

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
             extsched_str +
             "-M #{mem * 1000} -oo #{log} #{cmd_str}")
    end

    # Helper method for executing an asynchronous task array. See
    # async_task_array.
    def async_task_array(method_name, margs_arrays, commands, array_file,
        async_command, env, callbacks = {})
      pre, post, val = ensure_callbacks(callbacks)
      memos = Percolate.memoizer.async_method_memos(method_name)

      # If first in array was submitted, all were submitted
      submitted = memos.has_key?(margs_arrays.first) &&
          memos[margs_arrays.first].submitted?

      log = Percolate.log
      log.debug("Entering task #{method_name}")

      results = Array.new(margs_arrays.size)

      if submitted
        margs_arrays.each_with_index do |args, i|
          results[i] = memos[args]
          log.debug("Checking #{method_name}[#{i}] args: #{args.inspect}, " +
                        "result: #{results[i]}")

          update_result(method_name, args, post, val, results[i], log, i)
        end
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
            margs_arrays.each_with_index do |args, i|
              task_id = task_identity(method_name, *args)
              result = Result.new(method_name, :async, task_id, submission_time)
              memos[args] = result
              log.debug("Submitted #{method_name}[#{i}] args: #{args.inspect}, " +
                            "result #{result}")
            end
          end
        end
      end

      results
    end

    private
    def count_lines(file) # :nodoc
      count = 0
      File.open(file, 'r').each { |line| count = count + 1 }
      count
    end
  end
end
