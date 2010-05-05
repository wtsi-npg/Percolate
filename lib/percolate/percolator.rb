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

require 'yaml'
require 'optparse'
require 'logger'

module Percolate
  class PercolatorArguments < Hash
    def initialize args
      super

      opts = OptionParser.new do |opts|
        opts.banner = "Usage: #$0 [options]"
        opts.on('-c', '--config [FILE]',
                'Load Percolator configuration from FILE') do |file|
          self[:config] = file
        end

        opts.on('-l', '--list [GROUP]', 'List available workflows') do |group|
          begin
            $stderr.puts Percolate.find_workflows group
          rescue ArgumentError => ae
            $stderr.puts "Unknown workflow group #{group}"
          end
        end

        opts.on('-p', '--percolate', 'Run all workflows') do
          self[:percolate] = true
        end

        opts.on('-v', '--version', 'Print the Percolate version and exit') do
          $stderr.puts 'Version ' << VERSION
          exit
        end

        opts.on('-w', '--workflow [WORKFLOW]', 'Display workflow help') do |wf|
            begin
              klass = wf.split(/::/).inject(Object) { |m, c| m.const_get(c.to_sym) }

              if klass.respond_to?(:version)
                puts "#{klass.name} version #{klass.version}\n"
              end

              if klass.respond_to?(:description)
                puts "#{klass.description}\n"
              end

              if klass.respond_to?(:usage)
                puts "Usage:\n\n" << klass.usage
              end
            rescue NameError => ne
              $stderr.puts "Unknown workflow #{wf}"
            end
        end

        opts.on('-h', '--help', 'Display this help and exit') do
          $stderr.puts opts
          exit
        end
      end

      begin
        opts.parse args
      rescue OptionParser::ParseError => pe
        $stderr.puts opts
        $stderr.puts "\nInvalid argument: #{pe}"
      end

      self
    end
  end

  # The Percolator provides the entry point for running workflows via
  # its 'percolate' method. Instance variables are used to determine
  # the directories where it expects to find workflow definitions and
  # run files.
  class Percolator
    @@def_suffix = Workflow::DEFINITION_SUFFIX
    @@run_suffix = Workflow::RUN_SUFFIX

    attr_reader 'root_dir', 'lock_dir',
                'run_dir',  'pass_dir', 'fail_dir', 'work_dir', 'tmp_dir',
                'log_dir',  'log_file'

    # The config hash will normally be supplied via a YAML file on the
    # command line or a YAML .percolate file in the user's home
    # directory.
    def initialize config = {}
      symbol_config = {}
      config.each do |key, value|
        if value
          symbol_config[key.intern] = value
        end
      end

      root_dir = File.expand_path '~/percolate'
      tmp_dir = File.join((ENV['TMPDIR'] || '/tmp'), ENV['USER'])

      defaults = {:root_dir  => root_dir,
                  :tmp_dir   => tmp_dir,
                  :work_dir  => tmp_dir,
                  :log_dir   => root_dir,
                  :log_file  => 'percolate.log',
                  :log_level => 'WARN'}

      opts = defaults.merge(symbol_config)

      @root_dir = File.expand_path opts[:root_dir]
      @tmp_dir  = File.expand_path opts[:tmp_dir]
      @work_dir = File.expand_path opts[:work_dir]
      @log_dir  = opts[:log_dir]   || @root_dir
      @lock_dir = (opts[:lock_dir] || File.join(@tmp_dir, 'locks'))
      @run_dir  = (opts[:run_dir]  || File.join(@root_dir, 'in'))
      @pass_dir = (opts[:pass_dir] || File.join(@root_dir, 'pass'))
      @fail_dir = (opts[:fail_dir] || File.join(@root_dir, 'fail'))

      if FileTest.directory? opts[:log_file]
        raise ArgumentError,
              ":log_file must be a file name, not a directory: #{opts[:log_file]}"
      end

      begin
        [@tmp_dir, @lock_dir, @root_dir, @log_dir,
         @run_dir, @pass_dir, @fail_dir].map do |dir|
          if ! (File.exists?(dir) && File.directory?(dir))  # Dir.exists? dir
            Dir.mkdir dir
          end
        end
      rescue SystemCallError => se
        raise PercolateError, "Failed to create Percolate directories: #{se}"
      end

      log_level = Object.const_get('Logger').const_get(opts[:log_level])
      @log_file = File.join @log_dir, opts[:log_file]
      $log = Logger.new @log_file
      $log.level = log_level

      self
    end

    # Returns an array of workflow definition files.
    def find_definitions
      Dir[self.run_dir + '/*' + @@def_suffix]
    end

    # Returns an array of workflow run files.
    def find_run_files
      Dir[self.run_dir + '/*' + @@run_suffix]
    end

    # Returns an array of workflow definition files that do not have a
    # corresponding run file.
    def find_new_definitions
      defns = self.find_definitions.map do |file|
        File.basename file, @@def_suffix
      end
      runs = self.find_run_files.map do |file|
        File.basename file, @@run_suffix
      end

      (defns - runs).map do |basename|
        File.join self.run_dir, basename + @@def_suffix
      end
    end

    # Returns an array of workflow class and workflow arguments for
    # workflow definition in file.
    def read_definition file
      if ! File.exists? file
        raise PercolateError, "Workflow definition '#{file}' does not exist"
      end
      if ! File.file? file
        raise PercolateError, "Workflow definition '#{file}' is not a file"
      end
      if ! File.readable? file
        raise PercolateError, "Workflow definition '#{file}' is not readable"
      end

      begin
        $log.info "Loading workflow definition #{file} with #{self}"

        defn = YAML.load_file file
        lib = defn['library']
        if lib
          require lib
        end

        workflow_module = defn['group']
        workflow_class = defn['workflow']
        workflow_args = defn['arguments']

        if workflow_module.nil?
          raise ArgumentError,
                "Could not determine workflow module from definition '#{file}'"
        elsif workflow_class.nil?
          raise ArgumentError,
                "Could not determine workflow from definition '#{file}'"
        end

        processed_args =
                case workflow_args
                    when NilClass ; []
                    when String ; workflow_args.split
                    when Array ; workflow_args
                  else
                    raise ArgumentError,
                          "Expected an argument string, but found " <<
                          workflow_args.inspect
                end

        mod = Object.const_get(workflow_module)
        klass = mod.const_get(workflow_class)

        $log.info "Found workflow #{klass} with arguments #{processed_args.inspect}"

        [klass, processed_args]
      rescue ArgumentError => ae
        $stderr.puts "Error in workflow definiton '#{file}': #{ae}"
        nil
      rescue TypeError => te
        $stderr.puts "Error in workflow definiton '#{file}': #{te}"
        nil
      rescue NameError => ne
        $stderr.puts "Error in workflow definiton '#{file}': #{ne}"
        nil
      end
    end

    # Percolates data through the workflow described by definition.
    def percolate_tasks definition
      def_file = File.expand_path definition, self.run_dir
      run_file = def_file.gsub Regexp.new(File.extname(def_file) + '$'),
                               @@run_suffix
      lock_file = File.expand_path File.basename(definition, @@def_suffix),
                                   self.lock_dir

      # Prevent multiple processes working on the same workflow
      # concurrently.
      lock = File.new(lock_file, 'w')
      workflow = nil

      begin
        if lock.flock(File::LOCK_EX | File::LOCK_NB)
          begin
            $log.debug "Successfully obtained lock #{lock} for #{definition}"
            workflow_class, workflow_args = read_definition def_file
            workflow = workflow_class.new def_file, run_file,
                                          self.pass_dir, self.fail_dir

            # The following step is vital because all the memoization
            # data share the same namespace in the table. Without
            # clearing between workflows, workflow state would leak
            # from one workflow to another.
            $log.debug "Emptying memo table"
            Percolate::System.clear_memos

            if File.exists? run_file
              $log.info "Restoring state of #{definition} from #{run_file}"
              workflow.restore
            end

            # If we find a failed workflow, it means that it is being
            # restarted.
            if workflow.failed?
              workflow.restart
            end

            result = if ! workflow.finished?
                       workflow.run(*workflow_args)
                     else
                       nil
                     end

            $log.debug "Workflow run result is #{result.inspect}"

            if result
              $log.info "Workflow #{definition} passed"
              workflow.declare_passed
            else
              $log.info "Workflow #{definition} not passed; storing"
              workflow.store
            end
          rescue => e
            $log.error "Workflow #{definition} failed: #{e}"
            $log.error e.backtrace.join("\n")

            if workflow
              workflow.declare_failed
            end
          end
        else
          $log.debug "Busy lock #{lock} for #{definition}, skipping"
        end
      ensure
        if lock.flock(File::LOCK_UN).nonzero?
          raise PercolateError
                "Failed to release lock #{lock} for #{definition}"
        end
      end

      # Don't bother to remove the lock file if the workflow has not
      # finished.
      if workflow && (workflow.passed? || workflow.failed?)
        $log.debug "Deleting lock #{lock} for #{definition}"
        File.delete lock.path
      end

      workflow
    end

    # Percolates data through the currently active workflows.
    def percolate
      self.find_definitions.each do |defn|
        $log.info "Switched to workflow #{defn}"

        begin
          self.percolate_tasks defn
        rescue PercolateError => pe
          msg = "Skipping task: #{pe}"
          $log.error msg
          $stderr.puts msg
        end
      end
    end
  end
end
