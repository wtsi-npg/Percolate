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

require 'rubygems'
require 'test/unit'
require 'timeout'

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module AsyncTest
  include Percolate::Tasks

  def async_sleep(seconds, work_dir, log)
    margs = [seconds, work_dir]
    command = "sleep #{seconds}"

    async_task(margs, command, work_dir, log,
               :pre => lambda { work_dir },
               :post => lambda { true },
               :result => lambda { seconds },
               :unwrap => false,
               :async => {:queue => :small})
  end

  def p_async_sleep(seconds, size, work_dir, log)
    margs_arrays = size.times.collect { |i| [seconds + i, work_dir] }
    commands = size.times.collect { |i| "sleep #{seconds + i}" }

    async_task_array(margs_arrays, commands, work_dir, log,
                     :pre => lambda { work_dir },
                     :post => lambda { true },
                     :result => lambda { |sec, dir| sec },
                     :unwrap => false,
                     :async => {:queue => :small})
  end
end

module PercolateTest
  class TestAsyncWorkflow < Test::Unit::TestCase
    include Percolate
    include Tasks

    $LSF_PRESENT = system('which bsub >/dev/null 2>&1')

    def initialize(name)
      super(name)
      @msg_host = 'localhost'
      @msg_port = 11300
    end

    def bin_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'bin'))
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    class MinimalAsyncWorkflow < Workflow
      include AsyncTest

      def run(seconds, size, work_dir)
        log = nil

        size.times.collect { |i|
          async_sleep(seconds + i, work_dir, log)
        }
      end
    end

    class MinimalPAsyncWorkflow < Workflow
      include AsyncTest

      def run(seconds, size, work_dir, log)
        p_async_sleep(seconds, size, work_dir, log)
      end
    end

    def test_lsf_args
      asynchronizer = LSFAsynchronizer.new
      Percolate.asynchronizer = asynchronizer

      command = 'sleep 10'
      work_dir = data_path
      log = 'test_lsf_args.log'
      task_id = task_identity(:async_sleep, 10)

      array_file = File.join(data_path, 'test_lsf_args.txt')

      assert_raise ArgumentError do
        asynchronizer.async_command(task_id, command, data_path, log, :queue => :no_such_queue)
      end

      assert_raise ArgumentError do
        asynchronizer.async_command(task_id, command, data_path, log, :memory => -1)
      end

      assert_raise ArgumentError do
        asynchronizer.async_command(task_id, command, data_path, log, :memory => 0)
      end

      assert_raise ArgumentError do
        asynchronizer.async_command(task_id, command, data_path, log, :memory => nil)
      end
    end

    def test_minimal_async_workflow
      wrapper = File.join(bin_path, 'percolate-wrap')
      asynchronizer = SystemAsynchronizer.new(:async_wrapper => wrapper)
      Percolate.asynchronizer = asynchronizer

      percolator = Percolator.new({'root_dir' => data_path,
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'DEBUG',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port,
                                   'max_processes' => 2})

      wf = MinimalAsyncWorkflow.new(:dummy_def,
                                    'dummy_def.yml', 'dummy_run.run',
                                    percolator.pass_dir,
                                    percolator.fail_dir)
      asynchronizer.message_queue = wf.message_queue

      memoizer = Percolate.memoizer
      memoizer.clear_memos!
      assert(memoizer.async_result_count.zero?)
      assert(!memoizer.dirty_async?)
      run_time = 10
      size = 5

      # Initially nil from async task
      result = wf.run(run_time, size, '.')
      assert_equal(size.times.collect { nil }, result)

      # Test counting before updates
      assert_equal(memoizer.max_processes, memoizer.async_result_count)
      assert_equal(memoizer.max_processes,
                   memoizer.async_result_count { |r| r.submitted? })
      assert(memoizer.async_result_count { |r| r.started? }.zero?)
      assert(memoizer.async_result_count { |r| r.finished? }.zero?)
      assert(memoizer.dirty_async?)

      Timeout.timeout(60) do
        until result.collect { |r| r && r.finished? }.all? do
          memoizer.update_async_memos!
          result = wf.run(run_time, size, '.')

          sleep(5)
          print('#')
        end
      end

      # Test counting after updates
      assert_equal(size, memoizer.async_result_count)
      assert_equal(size, memoizer.async_result_count { |r| r.submitted? })
      assert_equal(size, memoizer.async_result_count { |r| r.started? })
      assert_equal(size, memoizer.async_result_count { |r| r.finished? })
      assert(!memoizer.dirty_async?)

      assert_equal([:async_sleep], result.collect { |r| r.task }.uniq)
      assert_equal(size.times.collect { |i| i + run_time },
                   result.collect { |r| r.value })
    end

    def test_minimal_p_async_workflow
      wrapper = File.join(bin_path, 'percolate-wrap')
      asynchronizer = LSFAsynchronizer.new(:async_wrapper => wrapper)
      Percolate.asynchronizer = asynchronizer

      if $LSF_PRESENT
        percolator = Percolator.new({'root_dir' => data_path,
                                     'log_file' => 'percolate-test.log',
                                     'log_level' => 'DEBUG',
                                     'msg_host' => @msg_host,
                                     'msg_port' => @msg_port})
        lsf_log = File.join(data_path, 'minimal_p_async_workflow.%I.log')

        wf = MinimalPAsyncWorkflow.new(:dummy_def,
                                       'dummy_def.yml', 'dummy_run.run',
                                       percolator.pass_dir,
                                       percolator.fail_dir)
        asynchronizer.message_queue = wf.message_queue

        memoizer = Percolate.memoizer
        memoizer.clear_memos!
        assert(memoizer.async_result_count.zero?)
        assert(!memoizer.dirty_async?)
        run_time = 5
        size = 5

        # Initially nil from async task
        result = wf.run(run_time, size, data_path, lsf_log)
        assert_equal(size.times.collect { nil }, result)

        # Test counting before updates
        # All jobs get submitted as one batch
        assert_equal(size, memoizer.async_result_count)
        assert_equal(size, memoizer.async_result_count { |r| r.submitted? })
        assert(memoizer.async_result_count { |r| r.started? }.zero?)
        assert(memoizer.async_result_count { |r| r.finished? }.zero?)
        assert(memoizer.dirty_async?)

        Timeout.timeout(60) do
          until result.collect { |r| r && r.finished? }.all? do
            memoizer.update_async_memos!
            result = wf.run(run_time, size, data_path, lsf_log)

            sleep(5)
          print('#')
          end
        end

        # Test counting after updates
        assert_equal(size, memoizer.async_result_count { |r| r.submitted? })
        assert_equal(size, memoizer.async_result_count { |r| r.started? })
        assert_equal(size, memoizer.async_result_count { |r| r.finished? })
        assert(!memoizer.dirty_async?)

        assert_equal([:p_async_sleep], result.collect { |r| r.task }.uniq)
        assert_equal(size.times.collect { |i| i + run_time },
                     result.collect { |r| r.value })
      end
    end

  end
end
