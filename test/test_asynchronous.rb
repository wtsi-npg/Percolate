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
require 'test/unit'
require 'socket'
require 'timeout'

devpath = File.expand_path(File.join(File.dirname(__FILE__), '..'))
libpath = File.join(devpath, 'lib')
testpath = File.join(devpath, 'test')

$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'
require File.join(testpath, 'helper')

module AsyncTest
  include Percolate::Tasks

  def async_sleep(seconds, work_dir, log)
    margs = [seconds, work_dir]
    command = "sleep #{seconds}"

    # The test queue is picked up from the LSF LSB_DEFAULQUEUE environment variable
    async_args =  {:memory => 100,
                   :priority => 99}

    async_task(margs, command, work_dir, log,
               :pre => lambda { work_dir },
               :post => lambda { true },
               :result => lambda { seconds },
               :unwrap => false,
               :async => async_args)
  end

  def p_async_sleep(seconds, size, work_dir, log, fail_index = nil)
    if fail_index && fail_index < 0
      raise ArgumentError, "fail_index must be >= 0"
    end
    if fail_index && fail_index >= size
      raise ArgumentError, "fail_index must be <= the job array size of #{size}"
    end

    margs_arrays = size.times.collect { |i| [seconds + i, work_dir] }
    commands = size.times.collect { |i| "sleep #{seconds + i}" }

    if fail_index
      commands[fail_index] = 'false'
    end

    # The test queue is picked up from the LSF LSB_DEFAULQUEUE environment variable
    async_args = {:memory => 100,
                  :priority => 99}

    # If the LSF data-aware extension is being used then add arguments to include
    # its use. This means testing to see whether the test environment has an
    # LSF storage location set
    storage_location = ENV['LSB_STORAGE_LOCATION']

    if storage_location && !storage_location.empty?
      async_args = async_args.merge({:storage => {:size => 1, :distance => 0}});
    end

    async_task_array(margs_arrays, commands, work_dir, log,
                     :pre => lambda { work_dir },
                     :post => lambda { true },
                     :result => lambda { |sec, dir| [sec, dir] },
                     :unwrap => false,
                     :async => async_args)
  end
end

module PercolateTest

  class TestAsyncWorkflow < Test::Unit::TestCase
    include Percolate
    include Tasks
    include Helper

    $LSF_PRESENT = system('which bsub >/dev/null 2>&1')

    def initialize(name)
      super(name)
      @msg_host = Socket.gethostname
      @msg_port = 11300
    end

    def setup
      Percolate.log = Logger.new(File.join(data_path, 'test_async_workflow.log'))
    end

    def bin_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'bin'))
    end

    def lib_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'lib'))
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    def tmp_path
      File.expand_path(File.join(ENV['HOME'], 'tmp'))
    end

    # We don't know which storage location LSF will use for the test job before
    # runtime.
    def storage_path
      File.join('$LSB_STORAGE_LOCATION', 'sanger', ENV['USER'])
    end

    class MinimalAsyncWorkflow < Workflow
      include AsyncTest

      def run(seconds, size, work_dir)
        log = nil

        results = size.times.collect { |i| async_sleep(seconds + i, work_dir, log) }
        results if results.collect { |result| result && result.value }.all?
      end
    end

    class MinimalPAsyncWorkflow < Workflow
      include AsyncTest

      def run(seconds, size, work_dir, log, fail_index = nil)
        results = p_async_sleep(seconds, size, work_dir, log, fail_index)
        results if results.collect { |result| result && result.value }.all?
      end
    end


    def test_lsf_args
      asynchronizer = LSFAsynchronizer.new

      command = 'sleep 10'
      log = 'test_lsf_args.log'
      task_id = task_identity(:async_sleep, 10)

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

    def test_lsf_default_queue
      asynchronizer = LSFAsynchronizer.new

      current_default = ENV.delete('LSB_DEFAULTQUEUE')
      assert_nil(asynchronizer.lsf_default_queue,
                 'if LSB_DEFAULTQUEUE is not set, the default queue is NIL')

      ENV['LSB_DEFAULTQUEUE'] = 'my_queue'
      assert_equal(:my_queue, asynchronizer.lsf_default_queue,
                   'default queue is the value of LSB_DEFAULTQUEUE')
      if (current_default)
        ENV['LSB_DEFAULTQUEUE'] = current_default
      end
    end

    def test_async_queues
      asynchronizer = LSFAsynchronizer.new
      assert(asynchronizer.async_queues.find(asynchronizer.lsf_default_queue))
    end

    def test_minimal_async_workflow
      work_dir = make_work_dir('test_minimal_async_workflow', data_path)

      asynchronizer = Percolate.asynchronizer
      asynchronizer.async_wrapper = File.join(bin_path, 'percolate-wrap')
      asynchronizer.ruby_args = {:I => lib_path}

      name = 'test_minimal_async_workflow'
      run_time = 10
      size = 5
      timeout = 120
      log = 'percolate.log'
      args = [run_time, size, work_dir]

      wf = test_workflow(name, PercolateTest::TestAsyncWorkflow::MinimalAsyncWorkflow,
                         timeout, work_dir, log, args,
                         :async => 'system', :max_processes => 2)
      assert(wf.passed?)

      # Test counting after updates
      memoizer = Percolate.memoizer
      assert_equal(size, memoizer.async_result_count)
      assert_equal(size, memoizer.async_result_count { |r| r.submitted? })
      assert_equal(size, memoizer.async_result_count { |r| r.started? })
      assert_equal(size, memoizer.async_result_count { |r| r.finished? })
      assert(!memoizer.dirty_async?)

       # Run once more, just to get the cached results
      results = wf.run(*args)

      assert_equal([:async_sleep], results.collect { |r| r.task }.uniq)
      assert_equal(size.times.collect { |i| i + run_time },
                   results.collect { |r| r.value })

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_minimal_p_async_workflow
      work_dir = make_work_dir('test_minimal_p_async_workflow', data_path)
      lsf_log = File.join(work_dir, 'minimal_p_async_workflow.%I.log')

      asynchronizer = Percolate.asynchronizer
      asynchronizer.async_wrapper = File.join(bin_path, 'percolate-wrap')
      asynchronizer.ruby_args = {:I => lib_path}

      name = 'test_minimal_p_async_workflow'
      run_time = 10
      size = 5
      timeout = 180
      log = 'percolate.log'
      args = [run_time, size, work_dir, lsf_log]

      wf = test_workflow(name, PercolateTest::TestAsyncWorkflow::MinimalPAsyncWorkflow,
                         timeout, work_dir, log, args,
                         :async => 'lsf', :max_processes => 250)
      assert(wf.passed?)

      # Test counting after updates
      memoizer = Percolate.memoizer
      assert_equal(size, memoizer.async_result_count)
      assert_equal(size, memoizer.async_result_count { |r| r.submitted? })
      assert_equal(size, memoizer.async_result_count { |r| r.started? })
      assert_equal(size, memoizer.async_result_count { |r| r.finished? })
      assert(!memoizer.dirty_async?)

      # Run once more, just to get the cached results
      results = wf.run(*args)

      assert_equal([:p_async_sleep], results.collect { |r| r.task }.uniq)
      assert_equal(size.times.collect { |i| i + run_time },
                   results.collect { |r| r.value.first })

      results.each do |r|
        value = maybe_unwrap(r, true)

        assert(value.respond_to?(:metadata))
        [:work_dir, :storage_location, :dataset].each do |key|
          assert(value.metadata.has_key?(key), "Metadata was missing key #{key}")
        end
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end
end
