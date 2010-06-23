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
require 'rubygems'

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module PercolateTest
  class TestPercolateSystem < Test::Unit::TestCase
    include Percolate
    include Percolate::System

    def setup
      super
      clear_memos
    end

    def teardown
      super
    end

    def test_get_memos
      memos = get_memos(:test_fn)

      assert(memos.is_a?(Hash))
      assert_equal(0, memos.size)
      assert(all_memos.has_key?(:test_fn))
    end

    def test_get_async_memos
      memos = get_async_memos(:test_async_fn)

      assert(memos.is_a?(Hash))
      assert_equal(0, memos.size)
      assert(all_async_memos.has_key?(:test_async_fn))
    end

    def test_store_restore_memos
      Dir.mktmpdir 'percolate' do |dir|
        file = File.join dir, 'store_restore_memos.dat'
        store_memos(file)
        data = restore_memos(file)

        assert_equal([all_memos, all_async_memos], data)
      end
    end

    def test_native_task
      def test_add_task *args
        having = lambda { |numbers| ! numbers.nil? }
        command = lambda { |*numbers| numbers.inject(0) { |n, sum| n + sum } }

        native_task(:test_add_task, args, command, having)
      end

      assert(! all_memos.has_key?(:test_add_task))

      result = test_add_task(1, 2, 3)
      assert_equal(:test_add_task, result.task)
      assert_equal(6, result.value)

      memos = get_memos(:test_add_task)
      assert(memos.is_a?(Hash))
      assert(all_memos.has_key?(:test_add_task))
    end
  end
end
