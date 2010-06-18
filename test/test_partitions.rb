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

module PercolateTest
  include Percolate

  class TestPartitions < Test::Unit::TestCase
    include Percolate

    def test_partitions
      assert_equal(['foo.part.0.txt'], partitions('foo.txt', 1))
      assert_equal(['foo.part.0.txt', 'foo.part.1.txt'],
                   partitions('foo.txt', 2))

      assert_equal(['/bar/foo.part.0.txt'], partitions('/bar/foo.txt', 1))
    end

    def test_partition?
      (0..100).each { |i| assert(partition?("foo.part.#{i}.txt")) }

      assert(partition? 'foo.part.0.txt')
      assert(partition? 'foo.part.100.txt')
      assert(partition? '999.part.0.txt')
      assert(partition? '999.part.0.999')

      assert(! partition?(nil))
      assert(! partition?('foo.part.0x.txt'))
      assert(! partition?('foo.part.x0.txt'))
      assert(! partition?('foo.part.x.txt'))
      assert(! partition?('foo.part.0.'))
      assert(! partition?('foo.part.0'))
      assert(! partition?('foo.part.'))
      assert(! partition?('foo.0.txt'))
    end

    def test_partition_index
      (0..100).each { |i| assert(i == partition_index("foo.part.#{i}.txt")) }

      assert_nil(partition_index nil)
      assert_raise ArgumentError do
        partition_index('foo.part.x.txt')
      end
    end

    def test_partition_parent
      (0..100).each do |i|
        assert('foo.txt' == partition_parent("foo.part.#{i}.txt"))
      end

      assert_nil(partition_parent nil)
      assert_raise ArgumentError do
        partition_parent('foo.part.x.txt')
      end
    end

    def test_partition_template
      (0..100).each do |i|
        assert('foo.part.%d.txt' == partition_template("foo.part.#{i}.txt"))
        assert('foo.part.x.txt' == partition_template("foo.part.#{i}.txt", 'x'))
      end

      assert_raise ArgumentError do
        partition_template(nil)
      end
      assert_raise ArgumentError do
        partition_template('foo.part.x.txt')
      end
    end

    def test_sibling_partitions?
      parts = (0...10).collect { |i| "foo.part.#{i}.txt" }
      assert(sibling_partitions?(parts))
      assert(! sibling_partitions?([]))
      assert(! sibling_partitions?([nil]))
      assert(! sibling_partitions?([nil, *parts]))
      assert(! sibling_partitions?(['foo.part.0.txt', *parts]))
      assert(! sibling_partitions?(['bar.part.0.txt', *parts]))
    end

    def test_complete_partitions?
      parts = (0...10).collect { |i| "foo.part.#{i}.txt" }
      assert(complete_partitions? parts)
      assert(! complete_partitions?([]))
      assert(! complete_partitions?([nil]))
      assert(! complete_partitions?([nil, *parts]))
      assert(! complete_partitions?(['foo.part.0.txt', *parts]))
      assert(! complete_partitions?(['bar.part.0.txt', *parts]))
      assert(! complete_partitions?(parts[1...10]))

      shuffled = parts
      until shuffled != parts
        shuffled = parts.shuffle
      end
      assert(complete_partitions? shuffled)

      assert_raise ArgumentError do
        complete_partitions?(['foo.part.x.txt'])
      end
    end
  end
end
