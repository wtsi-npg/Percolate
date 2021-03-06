#--
#
# Copyright (c) 2010-2013 Genome Research Ltd. All rights reserved.
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

require 'open3'

module Percolate
  # An Asynchronizer that submits jobs to platform LSF batch queues.
  class LSFAsynchronizer < TaskWrapper
    include Percolate
    include Utilities
    include Asynchronizer

    # The list of LSF queues as symbols
    attr_accessor :async_queues

    # The directory where LSF job array files will be written. This must be
    # visible to both the Percolator (which writes there) and the execution
    # hosts (which read those files). This is a hack; there should be some
    # sort of service that the job wrapper can look up what it's supposed to
    # run.
    attr_reader :job_arrays_dir

    # The LSF program which submits jobs to queues
    attr_reader :async_submitter

    # The LSF memory factor for the -M argument of bsub
    attr_reader :memory_factor

    # The LSF memory units
    attr_reader :memory_units

    def initialize(args = {})
      super(args)

      memory_units = find_memory_units('lsadmin')
      defaults = {:async_queues => find_lsf_queues('bqueues'),
                  :async_submitter => 'bsub',
                  :memory_units => memory_units,
                  :memory_factor => find_memory_factor(memory_units)}
      args = defaults.merge(args)

      @async_queues = args[:async_queues]
      @job_arrays_dir = args[:job_arrays_dir]
      @async_submitter = args[:async_submitter]

      # This depends on the LSF configuration LSF_UNIT_FOR_LIMITS
      @memory_factor = args[:memory_factor]
      @memory_units = args[:memory_units]
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
    #   - :host      => Limit to a specific host
    #   - :memory    => LSF memory limit in LSF_UNIT_FOR_LIMITS (Fixnum)
    #   - :priority  => LSF user priority
    #   - :depend    => LSF job dependency (String)
    #   - :select    => LSF resource select options (String)
    #   - :reserve   => LSF resource rusage options (String)
    #   - :storage   => LSF data-aware scheduling options (Hash) e.g.
    #     {:size => 100, :distance => 1} meaning select a node 1 unit
    #     away from 100Gb of free storage. May not be used in combination with
    #     the :dataset argument.
    #   - :dataset   => LSF data-aware scheduling dataset name (String) May not
    #     be used in combination with the :storage argument. The name may
    #     contain only the characters a-z, A-Z, 0-9, -, _ and .
    #   - :pre_exec  => LSF pre-exec command (String). Defaults to a test that
    #     the work_dir is mounted on the execution node.
    #   - :anchor    => Create an anchor job (Boolean). Defaults to true. Creates
    #     an extra /bin/true job to accompany each job array which depends on all
    #     the members of that array. This means that if any array job fails,
    #     the failed jobs stay in memory as long as the anchor job hasn't been
    #     finished.
    #
    # Returns:
    #
    # - String
    #
    def async_command(task_id, command, work_dir, log, args = {})
      defaults = {:queue => lsf_default_queue(),
                  :host => nil,
                  :memory => 1900,
                  :priority => nil,
                  :cpus => 1,
                  :depend => nil,
                  :select => nil,
                  :reserve => nil,
                  :storage => {},
                  :dataset => nil,
                  :pre_exec => %Q{"echo '[ -e #{work_dir} ] && [ -d #{work_dir} ]' | /bin/sh"},
                  :anchor => true}

      args = defaults.merge(args)
      memory, cpus = args[:memory], args[:cpus]
      storage, dataset = args[:storage], args[:dataset]
      uid = $$
      sdistance = ssize = nil

      validate_args(args[:queue], memory, cpus, dataset)

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

      queue_str = host_str = cpu_str = pre_exec_str = depend_str = select_str = reserve_str = priority_str = ''

      if args[:queue]
        queue_str = "-q #{args[:queue]}"
      end
      if args[:select]
        select_str = " && #{args[:select]}"
      end
      if args[:reserve]
        reserve_str = ":#{args[:reserve]}"
      end
      if args[:depend]
        depend_str = "-w #{args[:depend]}"
      end
      if args[:priority]
        priority_str = "-sp #{args[:priority]}"
      end
      if args[:host]
        host_str = "-m #{args[:host]}"
      end
      if args[:pre_exec]
        pre_exec_str = "-E #{args[:pre_exec]}"
      end
      if cpus > 1
        cpu_str = "-n #{cpus} -R 'span[hosts=1]'"
      end

      cmd_str = command_string(task_id)
      job_name = "#{task_id}.#{uid}"
      anchor_name = "#{job_name}.anchor"
      anchor_dep = "#{job_name}"

      # Support the data-aware scheduling extension to LSF used at WTSI
      extsched_str = ''
      if sdistance && ssize
        cmd_str << ' --storage'
        extsched_str = "-extsched 'storage[size=#{ssize};distance=#{sdistance}]' "
      elsif dataset
        cmd_str << " --dataset #{dataset}"
        extsched_str = "-extsched 'dataset[name=#{dataset}]'"
      end

      if command.is_a?(Array)
        # In a job array the actual command is pulled from the job's command
        # array file using the LSF job index
        job_name << "[1-#{command.size}]"
        anchor_dep << "[1-#{command.size}]"
        array_file = File.join(self.job_arrays_dir, task_id + '.txt')
        cmd_str << " --index #{array_file}"

        unless log =~ /%I/
          raise ArgumentError,
                "LSF job array log '#{log}' does not countain " +
                    "a job index placeholder (%I): all jobs " +
                    "would attempt to write to the same file"
        end
      else
        # Otherwise the command is run directly
        cmd_str << " -- '#{command}'"
      end

      memory_request = (memory * @memory_factor).to_i
      submission_str = "#{self.async_submitter} -J '#{job_name}' " +
          "#{queue_str} #{host_str} #{priority_str} " +
          "-R 'select[mem>#{memory}#{select_str}] " +
          "rusage[mem=#{memory}#{reserve_str}]' " +
          "-M #{memory_request} " +
          "#{depend_str} #{cpu_str} #{extsched_str} #{pre_exec_str} -oo #{log} #{cmd_str}"

      anchor_str = "#{self.async_submitter} -J '#{anchor_name}' " +
          "#{queue_str} #{host_str} #{priority_str} " +
          "-w 'done(#{anchor_dep})' -o /dev/null /bin/true"

      # Add anchor jobs to enable easy brequeue (stops LSF forgetting any
      # completed jobs)
      if args[:anchor]
        submission_str << " ; #{anchor_str}"
      end

      if absolute_path?(work_dir)
        cd(work_dir, submission_str)
      else
        submission_str
      end
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

    #
    # Returns the default LSF queue as a symbol, or NIL if there is no default.
    #
    def lsf_default_queue()
      default = ENV['LSB_DEFAULTQUEUE']

      if default && !default.empty?
        default.to_sym
      end
    end

    private
    def find_lsf_queues(cmd)
      qnames = Array.new()

      pipe = IO.popen(cmd)
      if pipe
        pipe.each do |line|
          if pipe.lineno > 1
            a = line.split()
            qnames << a[0].to_sym
          end
        end
      end
      if qnames.empty?
        raise SystemCallError,
              "Failed to get a list of LSF queues with the ''#{cmd}' command"
      end

      qnames
    end

    def count_lines(file) # :nodoc
      count = 0
      File.open(file, 'r').each { |line| count = count + 1 }
      count
    end

    def validate_args(queue, mem, cpus, dataset)
      unless queue.nil? || self.async_queues.include?(queue)
        raise ArgumentError, ":queue is #{queue}, must be one of #{self.async_queues.inspect}"
      end
      unless mem.is_a?(Fixnum) && mem > 0
        raise ArgumentError, ":memory must be a positive Fixnum but was '#{mem}'"
      end
      unless cpus.is_a?(Fixnum) && cpus > 0
        raise ArgumentError, ":cpus must be a positive Fixnum but was '#{cpus}'"
      end
      if dataset && !dataset.match(/^[a-zA-Z0-9_\-.]+$/)
        raise ArgumentError,
              "Invalid dataset name '#{dataset}': names may contain only the " +
                  "characters a-z, A-Z, 0-9, -, _ and ."
      end
    end

    def find_memory_units(cmd)
      units = 'KB'

      Open3.popen3(cmd, 'showconf', 'lim') do |stdin, stdout, stderr|
        stdout.each do |line|
          if line.match(/LSF_UNIT_FOR_LIMITS\s*=\s*([a-zA-Z]+)/)
            units = Regexp.last_match(1)
          else
            next
          end
        end
      end

      if units.nil?
        raise SystemCallError,
              "Failed to find LSF_UNIT_FOR_LIMITS with the '#{cmd}' command"
      end

      units
    end

    def find_memory_factor(units)
      case units
        when 'KB'
          return 1000
        when 'MB'
          return 1
        when 'GB'
          return 0.001
        else
          return 1 # Defaults to MB
      end
    end
  end
end
