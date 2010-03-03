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
  ## All workflows must be subclassed from Workflow which provides the basic
  ## workflow management methods.
  class Workflow
    include Percolate

    attr_reader 'definition_file', 'run_file', 'pass_dir', 'fail_dir',
                'passed', 'failed'

    def initialize definition_file, run_file, pass_dir, fail_dir
      @definition_file = definition_file
      @run_file = run_file
      @pass_dir = pass_dir
      @fail_dir = fail_dir
      @passed = false
      @failed = false
    end

    ## Restores the workflow from its run file, if it exists. Returns the
    ## workflow.
    def restore
      if File.exists? self.run_file
        Percolate::System.restore_memos self.run_file
      else
        raise PercolateError, "Run file #{self.run_file} for #{self} does not exist"
      end

      self
    end

    ## Stores the workflow to its run file. Returns the workflow.
    def store
      $log.debug "Storing workflow in #{self.run_file}"
      Percolate::System.store_memos self.run_file
      self
    end

    ## Archives the workflow to directory, moving the definition and run files
    ## to that location. Returns the workflow.
    def archive directory
      begin
        if File.exists? self.run_file
          $log.debug "Archiving #{self.run_file} to #{directory}"
          FileUtils::mv self.run_file, directory
        end

        if File.exists? self.definition_file
          $log.debug "Archiving #{self.definition_file} to #{directory}"
          FileUtils::mv self.definition_file, directory
        end
      rescue Exception => e
        raise PercolateError,
               "Failed to archive workflow #{self} to '#{directory}': #{e.message}"
      end

      self
    end

    ## Runs the workflow through one iteration.
    def run *args
      raise PercolateError,
            "No run method defined for workflow class #{self.class}"
    end

    ## Archives the workflow to the pass directory. Returns the workflow.
    def declare_passed
      if self.passed?
        raise PercolateError "Cannot pass #{self} because it has already failed"
      end

      $log.debug "Workflow #{self} passed"
      @passed = true
      self.store
      self.archive self.pass_dir
    end

    ## Returns true if the workflow has passed (finished successfully).
    def passed?
      @passed
    end

    ## Archives the workflow to the fail directory. Returns the workflow.
    def declare_failed
      if self.failed?
        raise PercolateError "Cannot fail #{self} because it has already passed"
      end

      $log.debug "Workflow #{self} failed"
      @failed = true
      self.store
      self.archive self.fail_dir
    end

    ## Returns true if the workflow has failed (an error has been raised at
    ## some point).
    def failed?
      @failed
    end

    ## Returns true if the workflow has been run (has completed and either
    ## passed or failed
    def run?
      self.passed? or self.failed?
    end

    ## Returns the archived location of the definition file.
    def passed_definition_file
      File.join self.pass_dir, File.basename(self.definition_file)
    end

    ## Returns the archived location of the run file.
    def passed_run_file
      File.join self.pass_dir, File.basename(self.run_file)
    end

    ## Returns the archived location of the definition file.
    def failed_definition_file
      File.join self.fail_dir, File.basename(self.definition_file)
    end

    ## Returns the archived location of the run file.
    def failed_run_file
      File.join self.fail_dir, File.basename(self.run_file)
    end

    def to_s
      "#<#{self.class} #{self.definition_file} run?: #{self.run?} passed?: #{self.passed?}>"
    end
  end

  ## The empty workflow. This returns a true value when run and does nothing else.
  class EmptyWorkflow < Workflow
    def run *args
      true_task *args
    end
  end

  ## The failing workflow. This fails by running the Unix 'false' command.
  class FailingWorkflow < Workflow
    def run *args
      ## args ignored intentionally
      false_task
    end
  end
end
