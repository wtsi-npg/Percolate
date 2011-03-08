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

require 'tmpdir'
require 'test/unit'

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module PercolateTest
  class TestPercolateMemoize < Test::Unit::TestCase
    include Percolate

    def setup
      super
      Percolate.memoizer.clear_memos
    end

    def teardown
      super
    end

    def add_task *args
      having = lambda { |numbers| !numbers.nil? }
      command = lambda { |*numbers| numbers.inject(0) { |n, sum| n + sum } }

      native_task(:test_add_task, args, command, having)
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

    def test_memo_count
      memoizer = Percolate.memoizer
      assert(memoizer.memo_count.zero?)
      assert(add_task(1, 2, 3))
      assert_equal(1, memoizer.memo_count)
      assert(add_task(1, 2, 4))
      assert_equal(2, memoizer.memo_count)
    end

    def test_store_restore_memos
      Dir.mktmpdir('percolate') { |dir|
        file = File.join dir, 'store_restore_memos.dat'
        Percolate.memoizer.store_memos(file, :passed)
        state = Percolate.memoizer.restore_memos(file)

        assert_equal(:passed, state)
      }
    end

    def test_native_task
      memoizer = Percolate.memoizer
      assert(!memoizer.memos.has_key?(:test_add_task))

      result = add_task(1, 2, 3)
      assert_equal(:test_add_task, result.task)
      assert_equal(6, result.value)

      memos = memoizer.method_memos(:test_add_task)
      assert(memos.is_a?(Hash))
      assert(memoizer.memos.has_key?(:test_add_task))
    end
  end
end
