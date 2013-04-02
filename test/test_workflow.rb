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

require 'rubygems'
require 'fileutils'
require 'test/unit'

devpath = File.expand_path(File.join(File.dirname(__FILE__), '..'))
libpath = File.join(devpath, 'lib')
testpath = File.join(devpath, 'test')

$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'
require File.join(testpath, 'helper')

module PercolateTest
  include Percolate
  include Utilities

  $BOOLEAN_WORKFLOW = false

  # The unready workflow. Can never be run because its preconditions
  # are not satisfied.
  class UnreadyWorkflow < Workflow
    # A task which is permanently unready and can never be run.
    def unready_task(work_dir = '.')
      task([work_dir], 'true', work_dir,
           :pre => lambda { false },
           :result => lambda { true })
    end

    def run(*args)
      unready_task(*args)
    end
  end

  # The unfinished workflow. Can never finish because its
  # postconditions are not satisfied.
  class UnfinishedWorkflow < Workflow
    # A task which may be run, but which never finishes.
    def unfinished_task(work_dir = '.')
      task([work_dir], 'true', work_dir,
           :post => lambda { false },
           :result => lambda { true })
    end

    def run(*args)
      unfinished_task(*args)
    end
  end

  # Switchable workflow to that may be set to pass or fail.
  class BooleanWorkflow < Workflow
    def boolean_task(work_dir = '.')
      program = $BOOLEAN_WORKFLOW ? 'true' : 'false'

      task([work_dir], program, work_dir,
           :result => lambda { $BOOLEAN_WORKFLOW })
    end

    def run(*args)
      boolean_task(*args)
    end
  end

  # This one doesn't unwrap its return value
  class StayWrappedWorkflow < Workflow
    def stay_wrapped_task(work_dir = '.')
      task([work_dir], 'true', work_dir,
           :pre => lambda { work_dir },
           :result => lambda { true },
           :unwrap => false)
    end

    def run(*args)
      stay_wrapped_task(*args)
    end
  end

  class BadTaskWorkflow < Workflow
     def bad_arg_task(work_dir = '.')
       task([work_dir], 'true', work_dir,
            :pre => :not_a_proc,
            :post => lambda { false },
            :result => lambda { true })
     end

    def run(*args)
      bad_arg_task
    end
  end

  class TestWorkflow < Test::Unit::TestCase
    include Percolate
    include Percolate::Utilities
    include Helper

    def initialize(name)
      super(name)
      @msg_host = 'localhost'
      @msg_port = 11300
    end

    def setup
      super
      Percolate.log = Logger.new(File.join(data_path, 'test_workflow.log'))
      Percolate.memoizer.clear_memos!
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    def make_empty_workflow(work_dir)
      percolator = Percolator.new({'root_dir' => work_dir,
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      def_file = File.join(percolator.run_dir, 'test_def1_tmp.yml')
      run_file = File.join(percolator.run_dir, 'test_def1_tmp.run')

      FileUtils.cp(File.join(data_path, 'in', 'test_def1.yml'), def_file)

      EmptyWorkflow.new(:test_def1, def_file, run_file,
                        percolator.pass_dir, percolator.fail_dir)
    end

    def test_lookup_by_name
      assert(find_class('Percolate::EmptyWorkflow').is_a?(Class))
    end

    def test_bad_definition_suffix
      assert_raise DefinitionError do
        EmptyWorkflow.new(:foo, "foo.txt", "foo.run", "pass_dir", "fail_dir")
      end
    end

    def test_bad_definition_basename
      assert_raise DefinitionError do
        EmptyWorkflow.new(:foo, "fo.o.yml", "foo.run", "pass_dir", "fail_dir")
      end
    end

    def test_task_args
      assert_raise ArgumentError do
        wf = BadTaskWorkflow.new(:dummy)
        wf.run
      end
    end

    def test_run_not_overridden
      wf = Workflow.new(:dummy, 'no_such_def_file.yml', 'no_such_run_file.run',
                        'no_such_pass_dir', 'no_such_fail_dir')

      assert_raise CoreError do
        wf.run
      end
    end

    def test_missing_run_file
      wf = Workflow.new(:dummy, 'no_such_def_file.yml', 'no_such_run_file.run',
                        'no_such_pass_dir', 'no_such_fail_dir')

      assert_raise CoreError do
        wf.restore!
      end
    end

    def test_double_pass
      work_dir = make_work_dir('test_double_pass', data_path)
      wf = make_empty_workflow(work_dir)
      wf.run
      wf.declare_passed!
      assert(wf.passed?)

      assert_raise WorkflowError do
        wf.declare_passed!
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_double_fail
      work_dir = make_work_dir('test_double_fail', data_path)
      wf = make_empty_workflow(work_dir)
      wf.run
      wf.declare_failed!
      assert(wf.failed?)

      assert_raise WorkflowError do
        wf.declare_failed!
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_make_workflow
      work_dir = make_work_dir('test_make_workflow', data_path)
      percolator = Percolator.new({'root_dir' => work_dir,
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      def_file = File.join(percolator.run_dir, 'test_def1.yml')
      run_file = File.join(percolator.run_dir, 'test_def1.run')
      
      FileUtils.cp(File.join(data_path, 'in', 'test_def1.yml'), def_file)

      assert(percolator.read_definition(def_file))
      assert(Workflow.new(:test_def1, def_file, run_file,
                          percolator.pass_dir, percolator.fail_dir))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_run_workflow
      work_dir = make_work_dir('test_run_workflow', data_path)
      percolator = Percolator.new({'root_dir' => work_dir,
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      def_file = File.join(percolator.run_dir, 'test_def1_tmp.yml')
      run_file = File.join(percolator.run_dir, 'test_def1_tmp.run')

      FileUtils.cp(File.join(data_path, 'in', 'test_def1.yml'), def_file)

      wf = StayWrappedWorkflow.new(:test_def1, def_file, run_file,
                                   percolator.pass_dir, percolator.fail_dir)

      assert(!wf.run(nil)) # Should require the work_dir arg
      assert(wf.run)
      x = wf.run
      assert(x.is_a?(Result))
      assert_equal(:stay_wrapped_task, x.task)
      assert_equal(true, x.value)

      memos = Percolate.memoizer.method_memos(:stay_wrapped_task)
      assert(memos.has_key? ['.'])
      assert(memos[['.']].is_a?(Result))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_store_workflow
      work_dir = make_work_dir('test_store_workflow', data_path)
      wf = make_empty_workflow(work_dir)

      assert_equal(false, File.exists?(wf.run_file))
      wf.run
      assert(wf.store)
      assert(File.exists?(wf.run_file))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_restore_workflow
      work_dir = make_work_dir('test_restore_workflow', data_path)
      wf = make_empty_workflow(work_dir)
      wf.run
      wf.store

      memoizer = Percolate.memoizer
      memoizer.clear_memos!

      assert(wf.restore!)
      memos = memoizer.method_memos(:true_task)

      assert(memos.has_key? ['.'])
      assert(memos[['.']].is_a?(Result))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_passed_workflow
      work_dir = make_work_dir('test_passed_workflow', data_path)
      wf = make_empty_workflow(work_dir)
      wf.run
      wf.store

      assert(File.exists?(wf.definition_file))
      assert(File.exists?(wf.run_file))
      assert_equal(false, File.exists?(wf.passed_definition_file))
      assert_equal(false, File.exists?(wf.passed_run_file))

      assert(!wf.passed?)
      assert(!wf.failed?)
      wf.declare_passed!
      assert(wf.passed?)
      assert(!wf.failed?)

      assert_equal(false, File.exists?(wf.definition_file))
      assert_equal(false, File.exists?(wf.run_file))
      assert(File.exists?(wf.passed_definition_file))
      assert(File.exists?(wf.passed_run_file))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_failed_workflow
      work_dir = make_work_dir('test_failed_workflow', data_path)
      wf = make_empty_workflow(work_dir)
      wf.run
      wf.store

      assert(File.exists?(wf.definition_file))
      assert(File.exists?(wf.run_file))
      assert_equal(false, File.exists?(wf.failed_definition_file))
      assert_equal(false, File.exists?(wf.failed_run_file))

      assert(!wf.passed?)
      assert(!wf.failed?)
      wf.declare_failed!
      assert(!wf.passed?)
      assert(wf.failed?)

      assert_equal(false, File.exists?(wf.definition_file))
      assert_equal(false, File.exists?(wf.run_file))
      assert(File.exists?(wf.failed_definition_file))
      assert(File.exists?(wf.failed_run_file))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_fail_then_pass_workflow
      $BOOLEAN_WORKFLOW = false
      work_dir = make_work_dir('test_fail_then_pass_workflow', data_path)
      percolator = Percolator.new({'root_dir' => work_dir,
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      def_file = File.join(percolator.run_dir, 'test_def1_tmp.yml')
      run_file = File.join(percolator.run_dir, 'test_def1_tmp.run')

      FileUtils.cp(File.join(data_path, 'in', 'test_def1.yml'), def_file)
      wf = BooleanWorkflow.new(:test_def1, def_file, run_file,
                               percolator.pass_dir, percolator.fail_dir)

      begin
        wf.run
      rescue => e
        assert(!wf.passed?)
        wf.declare_failed!
        assert(wf.failed?)
        assert(File.exists?(wf.failed_run_file))

        # Move the run file back to try re-running
        FileUtils.cp(wf.failed_run_file, wf.run_file)

        $BOOLEAN_WORKFLOW = true
        assert(wf.restore!)

        wf.restart!
        assert(wf.run)

        memos = Percolate.memoizer.method_memos(:boolean_task)
        assert(memos.has_key? ['.'])
        assert(memos[['.']].is_a?(Result))
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_unready_workflow
      work_dir = make_work_dir('test_unready_workflow', data_path)
      percolator = Percolator.new({'root_dir' => work_dir,
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      def_file = File.join(percolator.run_dir, 'test_def1.yml')
      run_file = File.join(percolator.run_dir, 'test_def1.run')

      wf = UnreadyWorkflow.new(:dummy, def_file, run_file,
                               percolator.pass_dir, percolator.fail_dir)
      assert_nil(wf.run)
      assert(!Percolate.memoizer.method_memos(:unready_task).has_key?(['.']))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_unfinished_workflow
      work_dir = make_work_dir('test_unfinished_workflow', data_path)
      percolator = Percolator.new({'root_dir' => data_path(),
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      def_file = File.join(percolator.run_dir, 'test_def1.yml')
      run_file = File.join(percolator.run_dir, 'test_def1.run')

      wf = UnfinishedWorkflow.new(:dummy, def_file, run_file,
                                  percolator.pass_dir, percolator.fail_dir)
      assert_nil(wf.run)
      assert(!Percolate.memoizer.method_memos(:unfinished_task).has_key?(['.']))

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_find_workflows
      assert([EmptyWorkflow, FailingWorkflow,
              Workflow].to_set.subset?(Percolate.find_workflows.to_set))
    end

    def test_transient_workflow
      wf = EmptyWorkflow.new(:test_def1)

      assert(wf.transient?)

      assert_raise WorkflowError do
        wf.declare_passed!
      end

      assert_raise WorkflowError do
        wf.declare_failed!
      end

      assert_raise WorkflowError do
        wf.passed_definition_file
      end

      assert_raise WorkflowError do
        wf.passed_run_file
      end

      assert_raise WorkflowError do
        wf.failed_definition_file
      end

      assert_raise WorkflowError do
        wf.failed_run_file
      end

      assert_raise WorkflowError do
        wf.store
      end

      assert_raise WorkflowError do
        wf.restore!
      end
    end
  end
end
