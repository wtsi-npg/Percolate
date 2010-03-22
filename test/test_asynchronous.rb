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

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module AsyncTest
  include Percolate

  def async_sleep seconds, work_dir, log, env = {}
    command = "sleep #{seconds}"

    lsf_task :async_sleep, [seconds, work_dir],
           Percolate.cd(work_dir, lsf(:async_sleep, $$, command, log,
                                      :queue => :test)), env, log,
           :having   => lambda { work_dir },
           :confirm  => lambda { true },
           :yielding => lambda { seconds }
  end
end

module PercolateTest
  class TestWorkflow < Test::Unit::TestCase
    include Percolate

    $LSF_PRESENT = system 'which bsub >/dev/null 2>&1'

    def data_path
      File.expand_path File.join File.dirname(__FILE__), '..', 'data'
    end

    class MinimalAsyncWorkflow < Workflow
      include AsyncTest

      def run *args
        async_sleep *args
      end
    end

    def test_lsf_run_success?
      assert_nil(lsf_run_success? 'no_such_file')
      assert_nil(lsf_run_success? File.join data_path, 'lsf_incomplete.log')

      assert_raise PercolateAsyncTaskError do
        lsf_run_success?(File.join data_path,
                         'lsf_unsuccessful_complete.log')
      end

      assert(lsf_run_success?(File.join data_path, 'lsf_successful_complete.log'))
    end

    def test_lsf_args
      command = 'sleep 10'
      log = 'test_lsf_args.log'

      assert_raise ArgumentError do
        lsf(:async_sleep, $$, command, log, :queue => :no_such_queue)
      end

      assert_raise ArgumentError do
        lsf(:async_sleep, $$, command, log, :memory => -1)
      end

      assert_raise ArgumentError do
        lsf(:async_sleep, $$, command, log, :memory => 0)
      end

      assert_raise ArgumentError do
        lsf(:async_sleep, $$, command, log, :memory => nil)
      end

      assert_raise ArgumentError do
        lsf(:async_sleep, $$, command, log, :size => -1)
      end

      assert_raise ArgumentError do
        lsf(:async_sleep, $$, command, log, :size => 0)
      end

      assert_raise ArgumentError do
        lsf(:async_sleep, $$, command, log, :size => nil)
      end
    end

    def test_minimal_async_workflow
      if $LSF_PRESENT
        begin
          percolator = Percolator.new({'root_dir' => data_path,
                                       'log_file' => 'percolate-test.log'})
          log_file = File.join data_path, 'minimal_async_workflow.log'

          wf = MinimalAsyncWorkflow.new 'dummy_defn', 'dummy_run',
                                        percolator.pass_dir, percolator.fail_dir

          assert(! System.dirty_async?)
          run_time = 10

          # Initially nil from async task
          assert_nil(wf.run run_time, '.', log_file)
          assert(System.dirty_async?)

          Timeout.timeout(120) do
            until (lsf_run_success?(log_file)) do
              sleep 10
              print '#'
            end
          end

          # Pick up log file
          x = wf.run run_time, '.', log_file

          assert(! System.dirty_async?)
          assert(x.is_a? Percolate::Result)
          assert_equal(:async_sleep, x.task)
          assert_equal(run_time, x.value)

        ensure
          if File.exists? log_file
             File.delete log_file
          end
        end
      end
    end
  end
end
