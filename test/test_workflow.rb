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

require 'fileutils'
require 'test/unit'

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module PercolateTest
  include Percolate

  # The unready workflow. Can never be run because its preconditions
  # are not satisfied.
  class UnreadyWorkflow < Workflow
    include Percolate

    # A task which is permanently unready and can never be run.
    def unready_task work_dir = '.', env = {}
      task(:unready_task, [work_dir], Percolate.cd(work_dir, 'true'), env,
           :having   => lambda { false },
           :confirm  => lambda { true },
           :yielding => lambda { true })
    end

    def run *args
      unready_task(*args)
    end
  end

  # The unfinished workflow. Can never finish because its
  # postconditions are not satisfied.
  class UnfinishedWorkflow < Workflow
    include Percolate

    # A task which may be run, but which never finishes.
    def unfinished_task work_dir = '.', env = {}
      task(:unfinished_task, [work_dir], Percolate.cd(work_dir, 'true'), env,
           :having   => lambda { true },
           :confirm  => lambda { false },
           :yielding => lambda { true })
    end

    def run *args
      unfinished_task(*args)
    end
  end

  class TestWorkflow < Test::Unit::TestCase
    include Percolate
    include Percolate::Memoize

    def setup
      super
      clear_memos
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    def make_empty_workflow
      percolator = Percolator.new({'root_dir' => data_path})
      defn_file = File.join(percolator.run_dir, 'test_def1_tmp.yml')
      run_file = File.join(percolator.run_dir, 'test_def1_tmp.run')

      FileUtils.cp(File.join(percolator.run_dir, 'test_def1.yml'), defn_file)

      EmptyWorkflow.new(defn_file, run_file,
                        percolator.pass_dir, percolator.fail_dir)
    end

    def test_bad_definition_suffix
      assert_raise ArgumentError do
        EmptyWorkflow.new("foo.txt", "foo.run", "pass_dir", "fail_dir")
      end
    end

    def test_bad_definition_basename
      assert_raise ArgumentError do
        EmptyWorkflow.new("fo o.yml", "foo.run", "pass_dir", "fail_dir")
      end
    end

    def test_task_args
      def bad_arg_task work_dir = '.', env = {}
          task(:bad_arg_task, [work_dir], Percolate.cd(work_dir, 'true'), env,
               :having   => :not_a_proc,
               :confirm  => lambda { false },
               :yielding => lambda { true })
      end

      assert_raise ArgumentError do
        bad_arg_task
      end
    end

    def test_run_not_overridden
      wf = Workflow.new("no_such_defn_file.yml", "no_such_run_file.run",
                        "no_such_pass_dir", "no_such_fail_dir")

      assert_raise PercolateError do
        wf.run
      end
    end

    def test_missing_run_file
      wf = Workflow.new "no_such_defn_file.yml", "no_such_run_file.run",
                        "no_such_pass_dir", "no_such_fail_dir"

      assert_raise PercolateError do
        wf.restore
      end
    end

    def test_double_pass
      begin
        wf = make_empty_workflow
        wf.run
        wf.declare_passed
        assert(wf.passed?)

        assert_raise PercolateError do
          wf.declare_passed
        end

      ensure
        File.delete(wf.passed_definition_file)
        File.delete(wf.passed_run_file)
      end
    end

    def test_double_fail
      begin
        wf = make_empty_workflow
        wf.run
        wf.declare_failed
        assert(wf.failed?)

        assert_raise PercolateError do
          wf.declare_failed
        end

      ensure
        File.delete(wf.failed_definition_file)
        File.delete(wf.failed_run_file)
      end
    end

    def test_make_workflow
      percolator = Percolator.new({'root_dir' => data_path})
      defn_file = File.join(percolator.run_dir, 'test_def1.yml')
      run_file = File.join(percolator.run_dir, 'test_def1.run')
      defn = percolator.read_definition(defn_file)

      assert(Workflow.new(defn_file, run_file,
                          percolator.pass_dir, percolator.fail_dir))
    end

    def test_run_workflow
      begin
        wf = make_empty_workflow

        assert(! wf.run(nil)) # Should require the work_dir arg
        assert(wf.run)
        x = wf.run
        assert(x.is_a?(Result))
        assert_equal(:true_task, x.task)
        assert_equal(true, x.value)

        memos = get_memos(:true_task)
        assert(memos.has_key? ['.'])
        assert(memos[['.']].is_a?(Result))
      ensure
        File.delete(wf.definition_file)
      end
    end

    def test_store_workflow
      begin
        wf = make_empty_workflow

        assert_equal(false, File.exists?(wf.run_file))
        wf.run
        assert(wf.store)
        assert(File.exists?(wf.run_file))
      ensure
        File.delete(wf.definition_file)
        File.delete(wf.run_file)
      end
    end

    def test_restore_workflow
      begin
        wf = make_empty_workflow
        wf.run
        wf.store

        clear_memos

        assert(wf.restore)
        memos = get_memos(:true_task)
        assert(memos.has_key? ['.'])
        assert(memos[['.']].is_a?(Result))
      ensure
        File.delete(wf.definition_file)
        File.delete(wf.run_file)
      end
    end

    def test_passed_workflow
      begin
        wf = make_empty_workflow
        wf.run
        wf.store

        assert(File.exists?(wf.definition_file))
        assert(File.exists?(wf.run_file))
        assert_equal(false, File.exists?(wf.passed_definition_file))
        assert_equal(false, File.exists?(wf.passed_run_file))

        assert(! wf.passed?)
        assert(! wf.failed?)
        wf.declare_passed
        assert(wf.passed?)
        assert(! wf.failed?)

        assert_equal(false, File.exists?(wf.definition_file))
        assert_equal(false, File.exists?(wf.run_file))
        assert(File.exists?(wf.passed_definition_file))
        assert(File.exists?(wf.passed_run_file))
      ensure
        File.delete(wf.passed_definition_file)
        File.delete(wf.passed_run_file)
      end
    end

    def test_failed_workflow
      begin
        wf = make_empty_workflow
        wf.run
        wf.store

        assert(File.exists?(wf.definition_file))
        assert(File.exists?(wf.run_file))
        assert_equal(false, File.exists?(wf.failed_definition_file))
        assert_equal(false, File.exists?(wf.failed_run_file))

        assert(! wf.passed?)
        assert(! wf.failed?)
        wf.declare_failed
        assert(! wf.passed?)
        assert(wf.failed?)

        assert_equal(false, File.exists?(wf.definition_file))
        assert_equal(false, File.exists?(wf.run_file))
        assert(File.exists?(wf.failed_definition_file))
        assert(File.exists?(wf.failed_run_file))
      ensure
        File.delete(wf.failed_definition_file)
        File.delete(wf.failed_run_file)
      end
    end

    def test_unready_workflow
      percolator = Percolator.new({'root_dir' => data_path})
      defn_file = File.join(percolator.run_dir, 'test_def1.yml')
      run_file = File.join(percolator.run_dir, 'test_def1.run')

      wf = UnreadyWorkflow.new(defn_file, run_file,
                               percolator.pass_dir, percolator.fail_dir)
      assert_nil(wf.run)
      assert(! get_memos(:unready_task).has_key?(['.']))
    end

    def test_unfinished_workflow
      percolator = Percolator.new({'root_dir' => data_path})
      defn_file = File.join(percolator.run_dir, 'test_def1.yml')
      run_file = File.join(percolator.run_dir, 'test_def1.run')

      wf = UnfinishedWorkflow.new(defn_file, run_file,
                                  percolator.pass_dir, percolator.fail_dir)
      assert_nil(wf.run)
      assert(! get_memos(:unfinished_task).has_key?(['.']))
    end

    def test_find_workflows
      assert([EmptyWorkflow, FailingWorkflow,
              Workflow].to_set.subset?(Percolate.find_workflows.to_set))
    end
  end
end
