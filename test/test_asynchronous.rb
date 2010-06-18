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

require 'test/unit'
require 'timeout'
require 'rubygems'

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module AsyncTest
  include Percolate

  def async_sleep seconds, work_dir, log, env = {}
    command = "sleep #{seconds}"

    task_id = Percolate.task_identity(:async_sleep, [seconds, work_dir])
    lsf_task(:async_sleep, [seconds, work_dir],
             lsf(task_id, command, work_dir, log, :queue => :normal),
             env,
             :having   => lambda { work_dir },
             :confirm  => lambda { true },
             :yielding => lambda { seconds })
  end
end

module PercolateTest
  class TestWorkflow < Test::Unit::TestCase
    include Percolate

    $LSF_PRESENT = system('which bsub >/dev/null 2>&1')

    def bin_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'bin'))
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    class MinimalAsyncWorkflow < Workflow
      include AsyncTest

      def run *args
        async_sleep(*args)
      end
    end

    def test_lsf_run_success?
      assert_nil(lsf_run_success?('no_such_file'))
      assert_nil(lsf_run_success?(File.join data_path, 'lsf_incomplete.log'))

      assert_raise PercolateAsyncTaskError do
        lsf_run_success?(File.join data_path,
                         'lsf_unsuccessful_complete.log')
      end

      assert(lsf_run_success?(File.join data_path,
                              'lsf_successful_complete.log'))
    end

    def test_lsf_args
      command = 'sleep 10'
      work_dir = data_path
      log = 'test_lsf_args.log'
      task_id = Percolate.task_identity(:async_sleep, 10)

      assert_raise ArgumentError do
        lsf(task_id, command, data_path, log, :queue => :no_such_queue)
      end

      assert_raise ArgumentError do
        lsf(task_id, command, data_path, log, :memory => -1)
      end

      assert_raise ArgumentError do
        lsf(task_id, command, data_path, log, :memory => 0)
      end

      assert_raise ArgumentError do
        lsf(task_id, command, data_path, log, :memory => nil)
      end

      assert_raise ArgumentError do
        lsf(task_id, command, data_path, log, :size => -1)
      end

      assert_raise ArgumentError do
        lsf(task_id, command, data_path, log, :size => 0)
      end

      assert_raise ArgumentError do
        lsf(task_id, command, data_path, log, :size => nil)
      end
    end

    def test_minimal_async_workflow
       batch_wrapper(File.join(bin_path, 'percolate-wrap'))

      if $LSF_PRESENT
        percolator = Percolator.new({'root_dir'  => data_path,
                                     'log_file'  => 'percolate-test.log',
                                     'log_level' => 'DEBUG'})
        lsf_log = File.join(data_path, 'minimal_async_workflow.log')

        wf = MinimalAsyncWorkflow.new('dummy_defn.yml', 'dummy_run.run',
                                      percolator.pass_dir,
                                      percolator.fail_dir)
        Asynchronous.message_queue(wf.message_queue)

        assert(! System.dirty_async?)
        run_time = 10

        # Initially nil from async task
        assert_nil(wf.run(run_time, '.', lsf_log))
        assert(System.dirty_async?)

        Timeout.timeout(120) do
          until (System.async_run_finished?(:async_sleep,
                                            [run_time, '.'])) do
              System.update_async_memos
              sleep(10)
              print('#')
            end
          end

          # Pick up result
          x = wf.run(run_time, '.', lsf_log)

          assert(! System.dirty_async?)
          assert(x.is_a?(Result))
          assert(x.started?)
          assert(x.finished?)
          assert_equal(:async_sleep, x.task)
          assert_equal(run_time, x.value)
      end
    end
  end
end
