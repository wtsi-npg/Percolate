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
require 'tmpdir'
require 'test/unit'

devpath = File.expand_path(File.join(File.dirname(__FILE__), '..'))
libpath = File.join(devpath, 'lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module PercolateTest
  class TestPercolateMemoize < Test::Unit::TestCase
    include Percolate
    include Percolate::NamedTasks
    include Percolate::Tasks
    include LSFDataAware

    def setup
      super
      Percolate.log = Logger.new(File.join(data_path, 'test_percolate_memoize.log'))
      Percolate.memoizer.clear_memos!
    end

    def teardown
      super
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    def sum_task(*args)
      pre = lambda { |numbers| !numbers.nil? }
      result = lambda { |*numbers| numbers.inject(0) { |n, sum| n + sum } }

      native_task(args, :pre => pre, :result => result, :unwrap => false)
    end

    def test_get_memos
      memos = Percolate.memoizer.method_memos(:test_fn)

      assert(memos.is_a?(Hash))
      assert_equal(0, memos.size)
      assert(Percolate.memoizer.memos.has_key?(:test_fn))
    end

    def test_get_async_memos
      memos = Percolate.memoizer.async_method_memos(:test_async_fn)

      assert(memos.is_a?(Hash))
      assert_equal(0, memos.size)
      assert(Percolate.memoizer.async_memos.has_key?(:test_async_fn))
    end

    def test_result_count
      memoizer = Percolate.memoizer
      assert(memoizer.result_count.zero?)
      assert(sum_task(1, 2, 3))
      assert_equal(1, memoizer.result_count)
      assert_equal(1, memoizer.result_count { |r| r.submitted? })
      assert_equal(1, memoizer.result_count { |r| r.started? })
      assert_equal(1, memoizer.result_count { |r| r.finished? })

      assert(sum_task(1, 2, 4))
      assert_equal(2, memoizer.result_count)
      assert_equal(2, memoizer.result_count { |r| r.submitted? })
      assert_equal(2, memoizer.result_count { |r| r.started? })
      assert_equal(2, memoizer.result_count { |r| r.finished? })
    end

    def test_store_restore_memos
      Dir.mktmpdir('percolate') do |dir|
        file = File.join(dir, 'store_restore_memos.dat')
        memoizer = Percolate.memoizer
        memoizer.store_memos(file, :dummy_name, :passed)
        workflow, state = memoizer.restore_memos!(file)

        assert_equal(:dummy_name, workflow)
        assert_equal(:passed, state)
      end
    end

    def test_native_task
      memoizer = Percolate.memoizer
      assert(!memoizer.memos.has_key?(:sum_task))

      result = sum_task(1, 2, 3)
      assert_equal(:sum_task, result.task)
      assert_equal(6, result.value)

      memos = memoizer.method_memos(:sum_task)
      assert(memos.is_a?(Hash))
      assert(memoizer.memos.has_key?(:sum_task))
    end

    def test_store_restore_dataset
      Dir.mktmpdir('percolate') do |dir|
        file = File.join(dir, 'store_restore_dataset.dat')
        memoizer = Percolate.memoizer
        assert(!memoizer.registered_datasets.has_key?(:test_dataset))
        memoizer.registered_datasets[:test_dataset] = 'test_dataset_location'

        memoizer.store_memos(file, :dummy_name, nil)
        workflow, state = memoizer.restore_memos!(file)

        assert(memoizer.registered_datasets.has_key?(:test_dataset))
      end
    end

  end
end
