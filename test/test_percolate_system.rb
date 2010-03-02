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
  class TestPercolateSystem < Test::Unit::TestCase

    def setup
      super
      Percolate::System.clear_memos
    end

    def teardown
      super
    end

    def test_get_memos
      memos = Percolate::System.get_memos(:test_fn)

      assert(memos.is_a? Hash)
      assert_equal(0, memos.size)
      assert($MEMOS.has_key? :test_fn)
    end

    def test_get_async_memos
      memos = Percolate::System.get_async_memos(:test_async_fn)

      assert(memos.is_a? Hash)
      assert_equal(0, memos.size)
      assert($ASYNC_MEMOS.has_key? :test_async_fn)
    end

    def test_store_restore_memos
      Percolate::System.get_memos(:test_fn)
      Percolate::System.get_async_memos(:test_async_fn)

      Dir.mktmpdir 'percolate' do |dir|
        file = File.join dir, 'store_restore_memos.dat'
        Percolate::System.store_memos file
        data = Percolate::System.restore_memos file

        assert_equal([$MEMOS, $ASYNC_MEMOS], data)
      end
    end
  end
end
