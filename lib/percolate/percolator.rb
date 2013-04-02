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

require 'optparse'
require 'logger'
require 'socket'
require 'yaml'

module Percolate
  class PercolatorArguments < Hash
    def initialize(args)
      super

      opts = OptionParser.new do |opts|
        opts.banner = "Usage: #$0 [options]"
        opts.on('-c', '--config [FILE]',
                'Load Percolator configuration from FILE') do |file|
          self[:config] = file
        end

        opts.on('-l', '--load LIBRARY', 'Load a workflow library') do |lib|
          begin
            self[:load] = lib
            require lib
          rescue LoadError
            puts("Could not load workflow library '#{lib}'")
            exit(CLI_ERROR)
          end
        end

        opts.on('-d', '--display [GROUP]',
                'Display available workflows') do |group|
          begin
            $stderr.puts(Percolate.find_workflows(group))
          rescue ArgumentError => ae
            $stderr.puts("Unknown workflow group #{group}")
            exit(CLI_ERROR)
          end

          exit
        end

        opts.on('-p', '--percolate', 'Run all defined workflows') do
          self[:percolate] = true
        end

        opts.on('-v', '--version', 'Print the Percolate version and exit') do
          $stderr.puts('Version ' + VERSION)
          exit
        end

        opts.on('-w', '--workflow [WORKFLOW]', 'Display workflow help') do |wf|
          begin
            klass = wf.split(/::/).inject(Object) { |m, c| m.const_get(c.to_sym) }

            if klass.respond_to?(:version)
              puts("#{klass.name} version #{klass.version}\n")
            end

            if klass.respond_to?(:description)
              puts("#{klass.description}\n")
            end

            if klass.respond_to?(:usage)
              puts("Usage:\n\n" + klass.usage)
            end
          rescue NameError => ne
            $stderr.puts "Unknown workflow #{wf}"
            exit(CLI_ERROR)
          end

          exit
        end

        opts.on('-h', '--help', 'Display this help and exit') do
          $stderr.puts(opts)
          exit
        end
      end

      begin
        opts.parse(args)

        if !self.has_key?(:percolate) && !self.has_key?(:load)
          raise ArgumentError, "a --load argument is required"
        end
      rescue SystemExit => se
        raise se
      rescue OptionParser::ParseError => pe
        $stderr.puts(opts)
        $stderr.puts("\nInvalid argument: #{pe}")
        exit(CLI_ERROR)
      rescue Exception => e
        $stderr.puts(opts)
        $stderr.puts("\nCommand line error: #{e}")
        exit(CLI_ERROR)
      end

      self
    end
  end

  # The Percolator provides the entry point for running workflows via
  # its 'percolate' method. Instance variables are used to determine
  # the directories where it expects to find workflow definitions and
  # run files.
  class Percolator
    include Utilities
    
    URI_REGEXP = URI.regexp(['file', 'urn'])

    # The root of all the Percolate runtime directories. Defaults to
    # $HOME/percolate
    attr_reader :root_dir
    # The directory where lock files are created
    attr_reader :lock_dir
    # The directory where running workflow definitions are to be placed
    attr_reader :run_dir
    # The directory where completed (passed) workflow definitions are
    # to be placed
    attr_reader :pass_dir
    # The directory where completed (failed) workflow definitions are
    # to be placed
    attr_reader :fail_dir
    # The working directory for workflows
    attr_reader :work_dir
    # The tmp file directory for workflows. Defaults to /tmp/<username>
    attr_reader :tmp_dir
    # The directory where log files are to be placed
    attr_reader :log_dir
    # The name of the Percolate log file
    attr_reader :log_file

    attr_reader :job_arrays_dir

    attr_reader :def_suffix
    attr_reader :run_suffix

    # The config hash will normally be supplied via a YAML file on the
    # command line or a YAML .percolate file in the user's home
    # directory.
    def initialize(config = {})
      sconfig = {}
      config.each { |key, value| sconfig[key.intern] = value if value }

      root_dir = File.expand_path('~/percolate')
      tmp_dir = File.join((ENV['TMPDIR'] || '/tmp'), ENV['USER'])

      defaults = {:root_dir => root_dir,
                  :tmp_dir => tmp_dir,
                  :work_dir => tmp_dir,
                  :log_dir => root_dir,
                  :log_file => 'percolate.log',
                  :log_level => 'WARN'}

      opts = defaults.merge(sconfig)

      # If the user has moved the root dir, but not defined a log dir,
      # move the log_dir too
      opts[:log_dir] = opts[:root_dir] if sconfig[:root_dir] && !sconfig[:log_dir]

      @root_dir = File.expand_path(opts[:root_dir])
      @tmp_dir = File.expand_path(opts[:tmp_dir])
      @work_dir = File.expand_path(opts[:work_dir])
      @log_dir = File.expand_path(opts[:log_dir]) || @root_dir
      @lock_dir = (opts[:lock_dir] || File.join(@tmp_dir, 'locks'))
      @run_dir = (opts[:run_dir] || File.join(@root_dir, 'in'))
      @pass_dir = (opts[:pass_dir] || File.join(@root_dir, 'pass'))
      @fail_dir = (opts[:fail_dir] || File.join(@root_dir, 'fail'))

      @job_arrays_dir = (opts[:job_arrays_dir] || File.join(@root_dir, 'job_arrays'))

      if FileTest.directory?(opts[:log_file])
        raise ArgumentError,
              ":log_file must be a file name, not a directory: " +
                  "#{opts[:log_file]}"
      end

      begin
        [@tmp_dir, @lock_dir, @root_dir, @log_dir,
         @run_dir, @pass_dir, @fail_dir, @job_arrays_dir].map do |dir|
          Dir.mkdir(dir) if !(File.exists?(dir) && File.directory?(dir))
        end
      rescue SystemCallError => se
        raise CoreError, "Failed to create Percolate directories: #{se}"
      end

      @log_file = File.join(@log_dir, opts[:log_file])
      Percolate.log = Logger.new(@log_file)
      Percolate.log.level = Object.const_get('Logger').const_get(opts[:log_level])

      msg_host = (opts[:msg_host] || Socket.gethostname)
      async = (opts[:async] || 'system')
      asynchronizer = make_asynchronizer(async)
      asynchronizer.message_host = msg_host
      asynchronizer.message_port = opts[:msg_port] if opts[:msg_port]
      Percolate.asynchronizer = asynchronizer
      Percolate.memoizer.max_processes = opts[:max_processes] if opts[:max_processes]

      @def_suffix = Workflow::DEFINITION_SUFFIX
      @run_suffix = Workflow::RUN_SUFFIX

      self
    end

    # Returns an array of workflow definition files.
    def find_definitions
      Dir[File.join(self.run_dir, '*') + self.def_suffix].sort
    end

    # Returns an array of workflow run files.
    def find_run_files
      Dir[File.join(self.run_dir, '*') + self.run_suffix].sort
    end

    # Returns an array of workflow definition files that do not have a
    # corresponding run file.
    def find_new_definitions
      defns = self.find_definitions.map do |file|
        File.basename(file, self.def_suffix)
      end
      runs = self.find_run_files.map do |file|
        File.basename(file, self.run_suffix)
      end

      (defns - runs).map do |basename|
        File.join(self.run_dir, basename + self.def_suffix)
      end
    end

    # Returns an array of workflow class and workflow arguments for
    # workflow definition in file.
    def read_definition(file)
      if !File.exists?(file)
        raise DefinitionError.new("Workflow definition '#{file}' does not exist", file)
      end
      if !File.file?(file)
        raise DefinitionError.new("Workflow definition '#{file}' is not a file", file)
      end
      if !File.readable?(file)
        raise DefinitionError.new("Workflow definition '#{file}' is not readable", file)
      end

      begin
        Percolate.log.info("Loading workflow definition #{file} with #{self}")

        defn = YAML.load_file(file)
        lib = defn['library']
        require lib if lib

        workflow_class = defn['workflow']
        workflow_args = defn['arguments']

        if workflow_class.nil?
          raise DefinitionError.new("Workflow missing from definition '#{file}'", file)
        end

        processed_args = case workflow_args
                           when NilClass ; []
                           when String ; workflow_args.split
                           when Array ; workflow_args
                           else
                             raise ArgumentError,
                                   "Expected an argument string, but found " +
                                       workflow_args.inspect
                         end

        klass = find_class(workflow_class)
        Percolate.log.info("Found workflow #{klass} with arguments " +
                               "#{processed_args.inspect}")

        [klass, processed_args]
      rescue ArgumentError => ae
        raise DefinitionError.new("Error in workflow definiton '#{file}': #{ae}", file)
      rescue TypeError => te
        raise DefinitionError.new("Error in workflow definiton '#{file}': #{te}", file)
      rescue NameError => ne
        raise DefinitionError.new("Error in workflow definiton '#{file}': " +
                                  "does workflow #{workflow_class} really exist? : #{ne}", file)
      end
    end

    # Percolates data through the currently active workflows.
    def percolate
      self.find_definitions.collect do |defn|
        Percolate.log.info("Switched to workflow #{defn}")

        begin
          self.percolate_tasks(defn)
        rescue PercolateError => pe
          msg = "Skipping task: #{pe}"
          Percolate.log.error(msg)
          $stderr.puts(msg)
        end
      end
    end

    # Percolates data through the workflow described by definition.
    def percolate_tasks(definition)
      def_file = File.expand_path(definition, self.run_dir)
      run_file = def_file.gsub(Regexp.new(File.extname(def_file) + '$'),
                               self.run_suffix)
      lock_file = File.expand_path(File.basename(definition, self.def_suffix),
                                   self.lock_dir)

      # Prevent multiple processes working on the same workflow
      # concurrently.
      lock = File.new(lock_file, 'w')
      workflow = nil

      memoizer = Percolate.memoizer
      log = Percolate.log

      begin
        if lock.flock(File::LOCK_EX | File::LOCK_NB)
          begin
            log.debug("Successfully obtained lock #{lock} for #{definition}")
            workflow_class, workflow_args = read_definition(def_file)
            workflow = workflow_class.new(File.basename(def_file, '.yml'),
                                          def_file, run_file,
                                          self.pass_dir, self.fail_dir)

            # The following step is vital because all the memoization
            # data share the same namespace in the table. Without
            # clearing between workflows, workflow state would leak
            # from one workflow to another.
            memoizer.clear_memos!

            if File.exists?(run_file)
              log.info("Restoring state of #{definition} from #{run_file}")
              workflow.restore!
            end

            Percolate.asynchronizer.message_queue = workflow.message_queue
            memoizer.update_async_memos! if memoizer.dirty_async?

            # If we find a failed workflow, it means that it is being
            # restarted.
            if workflow.failed?
              memoizer.purge_async_memos!

              log.info("Restarting #{definition} [FAILED] from #{run_file}")
              workflow.restart!
            else
              log.info("Continuing #{definition} from #{run_file}")
            end

            result = unless workflow.finished?
                       workflow.run(*substitute_uris(workflow_args))
                     end

            log.debug("Workflow run result is #{result.inspect}")

            if result
              log.info("Workflow #{definition} passed")
              workflow.declare_passed! # Stores in pass directory
            else
              log.info("Workflow #{definition} not passed; storing")
              workflow.store
            end
          rescue => e
            log.error("Workflow #{definition} failed: #{e}")
            log.error(e.backtrace.join("\n"))

            if workflow
              workflow.declare_failed! # Stores in fail directory
            end
          end
        else
          log.debug("Busy lock #{lock} for #{definition}, skipping")
        end
      ensure
        if lock.flock(File::LOCK_UN).nonzero?
          raise CoreError,
                "Failed to release lock #{lock} for #{definition}"
        end
      end

      # Don't bother to remove the lock file if the workflow has not
      # finished.
      if workflow && (workflow.passed? || workflow.failed?)
        log.debug("Deleting lock #{lock} for #{definition}")
        File.delete(lock.path)
      end

      workflow
    end

    private
    def make_asynchronizer(type)
      case type.to_s.downcase
        when 'lsf'
          LSFAsynchronizer.new(:job_arrays_dir => self.job_arrays_dir)
        when 'system'
          SystemAsynchronizer.new
        else
          raise ArgumentError,
                "Invalid asynchronizer type '#{type}', expected one of " +
                    %w{lsf system}.inspect
      end
    end

    def substitute_uris(args) # :nodoc
      args.collect do |arg|
        if arg.is_a?(String) && URI_REGEXP.match(arg)
          URI.parse(arg.slice(URI_REGEXP))
        else
          arg
        end
      end
    end
  end
end
