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

  def async_sleep seconds, work_dir, log, env = { }
    command = "sleep #{seconds}"

    task_id = Percolate.task_identity(:async_sleep, [seconds, work_dir])
    lsf_task(:async_sleep, [seconds, work_dir],
             lsf(task_id, command, work_dir, log, :queue => :small),
             env,
             :having   => lambda { work_dir },
             :confirm  => lambda { true },
             :yielding => lambda { seconds })
  end

  def p_async_sleep seconds, size, work_dir, log, env = { }
    args_arrays = size.times.collect { |i| [seconds + i, work_dir] }
    commands = size.times.collect { |i| "sleep #{seconds + i}" }

    task_id = Percolate.task_identity(:p_async_sleep, args_arrays)
    log = "#{task_id}.%I.log"
    array_file = File.join(work_dir, "#{task_id}.txt")

    write_array_commands(array_file, :p_async_sleep, args_arrays, commands)

    lsf_task_array(:p_async_sleep, args_arrays, commands,
                   lsf(task_id, nil, work_dir, log, :array_file => array_file,
                       :queue => :small),
                   env,
                   :having   => lambda { work_dir },
                   :confirm  => lambda { true },
                   :yielding => lambda { |sec, dir| sec })
  end
end

module PercolateTest
  class TestWorkflow < Test::Unit::TestCase
    include Percolate
    include Percolate::Memoize

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

    class MinimalPAsyncWorkflow < Workflow
      include AsyncTest

      def run *args
        p_async_sleep(*args)
      end
    end

    def test_lsf_args
      command = 'sleep 10'
      work_dir = data_path
      log = 'test_lsf_args.log'
      task_id = Percolate.task_identity(:async_sleep, 10)

      array_file = File.join(data_path, 'test_lsf_args.txt')

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
        lsf(task_id, command, data_path, log, :array_file => array_file)
      end
    end

    def test_minimal_async_workflow
       batch_wrapper(File.join(bin_path, 'percolate-wrap'))

      if $LSF_PRESENT
        percolator = Percolator.new({'root_dir'  => data_path,
                                     'log_file'  => 'percolate-test.log',
                                     'log_level' => 'DEBUG',
                                     'msg_host'  => 'hgs3b',
                                     'msg_port'  => 11301})
        lsf_log = File.join(data_path, 'minimal_async_workflow.log')

        wf = MinimalAsyncWorkflow.new(:dumm_def,
                                      'dummy_def.yml', 'dummy_run.run',
                                      percolator.pass_dir,
                                      percolator.fail_dir)
        Asynchronous.message_queue(wf.message_queue)

        assert(! dirty_async?)
        run_time = 10

        # Initially nil from async task
        assert_nil(wf.run(run_time, '.', lsf_log))
        assert(dirty_async?)

        Timeout.timeout(120) do
          until (async_run_finished?(:async_sleep, [run_time, '.'])) do
              update_async_memos
              sleep(10)
              print('#')
            end
          end

          # Pick up result
          x = wf.run(run_time, '.', lsf_log)

          assert(! dirty_async?)
          assert(x.is_a?(Result))
          assert(x.started?)
          assert(x.finished?)
          assert_equal(:async_sleep, x.task)
          assert_equal(run_time, x.value)
      end
    end

    def test_minimal_p_async_workflow
       batch_wrapper(File.join(bin_path, 'percolate-wrap'))

      if $LSF_PRESENT
        percolator = Percolator.new({'root_dir'  => data_path,
                                     'log_file'  => 'percolate-test.log',
                                     'log_level' => 'DEBUG',
                                     'msg_host'  => 'hgs3b',
                                     'msg_port'  => 11301})
        lsf_log = File.join(data_path, 'minimal_p_async_workflow.log')

        wf = MinimalPAsyncWorkflow.new(:dummy_def, 
                                       'dummy_def.yml', 'dummy_run.run',
                                       percolator.pass_dir,
                                       percolator.fail_dir)
        Asynchronous.message_queue(wf.message_queue)

        assert(! dirty_async?)
        run_time = 10
        size = 5

        # Initially nil from async task
        assert_equal([nil, nil, nil, nil, nil],
                     wf.run(run_time, size, data_path, lsf_log))
        assert(dirty_async?)

        Timeout.timeout(120) do
          until (! size.times.collect { |i|
                   async_run_finished?(:p_async_sleep,
                                       [run_time + i, data_path])
                 }.include?(false)) do
              update_async_memos
              sleep(run_time)
              print('#')
            end
          end

        # Pick up result
        x = wf.run(run_time, size, data_path, lsf_log)

        assert(! dirty_async?)
        assert(x.is_a?(Array))
        assert(x.all? { |elt| elt.is_a?(Result) })
        assert(x.all? { |elt| elt.started? })
        assert(x.all? { |elt| elt.finished? })
        assert_equal([:p_async_sleep], x.collect { |elt| elt.task }.uniq)
        assert_equal([10, 11, 12, 13, 14], x.collect { |elt| elt.value })
      end
    end
  end
end
