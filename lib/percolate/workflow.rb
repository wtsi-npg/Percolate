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
  # All workflows must be subclassed from Workflow which provides the
  # basic workflow management methods.
  class Workflow
    include Percolate
    include Utilities
    include Tasks

    DEFINITION_SUFFIX = '.yml'
    RUN_SUFFIX = '.run'
    BASENAME_REGEXP = /^[^.]+$/
    STATES = [:passed, :failed, nil]

    # Metaclass which holds a different set of help strings for each
    # subclass.
    class << self
      # The description string for online user help
      def description(str = '<no description available>')
        @description ||= str
      end

      # The usage string for online user help
      def usage(str = '<no usage information available>')
        @usage ||= str
      end

      # The version string for online user help
      def version(str = '<no version information available>')
        @version ||= str
      end
    end

    # The unique workflow identity string
    attr_reader :workflow_identity
    # The definition file for the workflow
    attr_reader :definition_file
    # The run (state) file for the workflow
    attr_reader :run_file
    # The directory to which the files will be moved on success
    attr_reader :pass_dir
    # The directory to which the files will be moved on failure
    attr_reader :fail_dir

    def initialize(identity, definition_file = nil, run_file = nil,
                   pass_dir = nil, fail_dir = nil)
      unless identity.is_a?(String) || identity.is_a?(Symbol)
        raise ArgumentError,
              "Invalid identity '#{identity.inspect}'. " +
                  "Must be a String or Symbol."
      end

      if definition_file
        unless File.extname(definition_file) == DEFINITION_SUFFIX
          raise ArgumentError,
                "Invalid definition file name '#{definition_file}'. " +
                    "Suffix must be '#{DEFINITION_SUFFIX}'"
        end

        unless File.basename(definition_file,
                             File.extname(definition_file)).match(BASENAME_REGEXP)
          raise ArgumentError,
                "Invalid definition file name '#{definition_file}'. " +
                    "Basename must match '#{BASENAME_REGEXP.inspect}'"
        end
      end

      @workflow_identity = identity.to_s
      @definition_file = definition_file
      @run_file = run_file
      @pass_dir = pass_dir
      @fail_dir = fail_dir
      @passed = false
      @failed = false
    end

    # The description string for online user help.
    def description
      self.class.description
    end

    # The usage string for online user help.
    def usage
      self.class.usage
    end

    # The version string for online user help.
    def version
      self.class.version
    end

    # A transient workflow is one that does not have a definition
    # file. Transient workflows are created at runtime by other
    # workflows.
    def transient?
      self.definition_file.nil?
    end

    # Returns the basename of the definition file which is used as the
    # run name.
    def run_name
      File.basename(self.definition_file)
    end

    # Restores the workflow from its run file, if it exists. Returns
    # the workflow.
    def restore!
      check_transient(:restore)
      if File.exists?(self.run_file)
        workflow, state = Percolate.memoizer.restore_memos!(self.run_file)

        unless workflow == self.class
          raise PercolateError,
                "Attempted to restore a #{workflow} workflow from " +
                    "#{self.run_file} into #{self}, which is a #{self.class} " +
                    "workflow"
        end

        Percolate.log.debug("Restored #{self} with state #{state}")

        case state
          when :passed;
            @passed = true
          when :failed;
            @failed = true
          when nil;
            nil
          else
            raise PercolateError,
                  "Bad state #{state} in #{self.run_file} for #{self}"
        end
      else
        raise PercolateError,
              "Run file #{self.run_file} for #{self} does not exist"
      end

      self
    end

    # Stores the workflow to its run file. Returns the workflow.
    def store
      check_transient(:store)
      state = case
                when self.passed?
                  :passed
                when self.failed?
                  :failed
                else
                  nil
              end

      Percolate.log.debug("Storing workflow in #{self.run_file}, state: #{state}")
      Percolate.memoizer.store_memos(self.run_file, self.class, state)
      self
    end

    # Archives the workflow to directory, moving the definition and
    # run files to that location. Returns the workflow.
    def archive(directory)
      begin
        self.store

        if File.exists?(self.run_file)
          Percolate.log.debug("Archiving #{self.run_file} to #{directory}")
          FileUtils.mv(self.run_file, directory)
        end

        if File.exists?(self.definition_file)
          Percolate.log.debug("Archiving #{self.definition_file} to #{directory}")
          FileUtils.mv(self.definition_file, directory)
        end
      rescue Exception => e
        raise PercolateError,
              "Failed to archive workflow #{self} to '#{directory}': " +
                  "#{e.message}"
      end

      self
    end

    # Runs the workflow through one iteration.
    def run(*args)
      raise PercolateError,
            "No run method defined for workflow class #{self.class}"
    end

    # Archives the workflow to the pass directory. Returns the
    # workflow.
    def declare_passed!
      check_transient(:declare_passed)
      if self.passed?
        raise PercolateError,
              "Cannot pass #{self} because it has already passed"
      end

      Percolate.log.debug("Workflow #{self} passed")
      @passed = true
      self.archive(self.pass_dir)
    end

    # Returns true if the workflow has passed (finished successfully).
    def passed?
      @passed
    end

    # Archives the workflow to the fail directory. Returns the
    # workflow.
    def declare_failed!
      check_transient(:declare_failed)
      if self.failed?
        raise PercolateError,
              "Cannot fail #{self} because it has already failed"
      end

      Percolate.log.debug("Workflow #{self} failed")
      @failed = true
      self.archive(self.fail_dir)
    end

    # Returns true if the workflow has failed (an error has been
    # raised at some point).
    def failed?
      @failed
    end

    # Returns true if the workflow has been run (has completed and
    # either passed or failed).
    def finished?
      self.passed? || self.failed?
    end

    # Restarts workflow by removing any pass or fail
    # information. Returns the workflow.
    def restart!
      check_transient(:restart)
      unless self.finished?
        raise PercolateError,
              "Cannot restart #{self} because it has not finished"
      end

      if self.passed?
        Percolate.log.debug("Restarting passed workflow #{self}")
        @passed = false
      elsif self.failed?
        Percolate.log.debug("Restarting failed workflow #{self}")
        @failed = false
      end

      self
    end

    # Returns the archived location of the definition file.
    def passed_definition_file
      check_transient(:passed_definition_file)
      File.join(self.pass_dir, File.basename(self.definition_file))
    end

    # Returns the archived location of the run file.
    def passed_run_file
      check_transient(:passed_run_file)
      File.join(self.pass_dir, File.basename(self.run_file))
    end

    # Returns the archived location of the definition file.
    def failed_definition_file
      check_transient(:failed_definition_file)
      File.join(self.fail_dir, File.basename(self.definition_file))
    end

    # Returns the archived location of the run file.
    def failed_run_file
      check_transient(:failed_run_file)
      File.join(self.fail_dir, File.basename(self.run_file))
    end

    # Returns the message queue name for this workflow.
    def message_queue
      identity = self.workflow_identity
      digest = Digest::MD5.hexdigest(identity)
      sanitize_queue_name("#{digest}-#{identity}".slice(0, 128))
    end

    def to_s
      state = self.finished? ? ' finished:' : nil
      result = case
                 when self.passed?
                   ' passed'
                 when self.failed?
                   ' failed'
                 else
                   nil
               end

      "#<#{self.class} #{self.definition_file}#{state}#{result}>"
    end

    private
    def check_transient(operation) # :nodoc
      if self.transient?
        raise PercolateError,
              "#{operation} cannot be performed on transient workflow #{self.to_s}"
      end
    end

    # The beanstalk queue protocol allows only certain characters in a queue
    # name. They may contain letters (A-Z and a-z), numerals (0-9),
    # hyphen ("-"), plus ("+"), slash ("/"), semicolon (";"), dot ("."),
    # dollar-sign ("$"), and parentheses ("(" and ")"), but they may not
    # begin with a hyphen. They are terminated by white space (either a
    # space char or end of line).
    def sanitize_queue_name(name)
      name.gsub(/[^A-Za-z0-9\-+\/;().$]|\s/, '-')
    end
  end
end
