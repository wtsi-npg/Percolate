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

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module AsyncTest
  include Percolate

  def async_sleep seconds, work_dir, log, env = {}
    command = "sleep #{seconds}"

    lsf_task :async_sleep, [seconds, work_dir],
           Percolate.cd(work_dir, lsf(:async_sleep, $$, command, log)), env,
           :having   => lambda { work_dir },
           :confirm  => lambda { lsf_run_success? log },
           :yielding => lambda { seconds }
  end
end

module PercolateTest
  class TestWorkflow < Test::Unit::TestCase
    include Percolate

    LSF_PRESENT = system 'which bsub'

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
      assert_equal(false, lsf_run_success?(File.join data_path,
                                           'lsf_unsuccessful_complete.log'))
      assert(lsf_run_success?(File.join data_path, 'lsf_successful_complete.log'))
    end

    def test_minimal_async_workflow
      if LSF_PRESENT
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

          # FIXME -- use timeout module
          # FIXME -- why do test failures avoid the ensure block?

          time = 0
          until (lsf_run_success?(log_file) || time > 60) do
            sleep 5
            time += 5
            print '#'
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
