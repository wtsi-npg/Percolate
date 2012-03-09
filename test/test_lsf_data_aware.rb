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

devpath = File.expand_path(File.join(File.dirname(__FILE__), '..'))
libpath = File.join(devpath, 'lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module PercolateTest

  class TestDAS < Test::Unit::TestCase
    include Percolate
    include Utilities
    include LSFDataAware

    def setup
      super
      Percolate.log = Logger.new(File.join(data_path, 'test_lsf_data_aware.log'))
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    def test_storage_root
      assert_equal("/lustre/scratch101/sanger/" + ENV['USER'],
                   storage_root("/lustre/scratch101"))
    end

    def test_abstract_path?
      assert(!abstract_path?("/foo"))
      assert(!abstract_path?("foo"))

      a = "/foo"
      a.extend(Metadata)
      a.metadata[:storage_location] = '/lustre/scratch101'
      assert(!abstract_path?(a))

      a = "foo"
      a.extend(Metadata)
      assert(!abstract_path?(a))

      a.metadata[:storage_location] = '/lustre/scratch101'
      assert(abstract_path?(a))
    end

    def test_concrete_path
      a = "foo"
      a.extend(Metadata)

      assert_raise ArgumentError do
        concrete_path(a)
      end

      a.metadata[:storage_location] = '/lustre/scratch101'

      assert_equal(File.join(storage_root("/lustre/scratch101"), a),
                   concrete_path(a))

      a.metadata[:dataset] = '/lustre/scratch101'
      assert_raise CoreError do
        concrete_path(a)
      end

      a.metadata.delete(:storage_location)

      assert_equal(File.join(storage_root("/lustre/scratch101"), a),
                   concrete_path(a))
    end

    def test_register_dataset
      name = 'percolate_test_dataset.' + Socket.gethostname + '.' + $$.to_s
      location = '/lustre/scratch101'

      if datactrl_available?
        assert_raise ArgumentError do
          register_dataset(nil, location)
        end

        assert_raise ArgumentError do
          register_dataset(name, nil)
        end

        assert_equal(name, register_dataset(name, location))

        assert_raise PercolateError do
          register_dataset(name, location)
        end

        assert(registered_dataset?(name))
        assert_equal(location, registered_datasets[name])

        assert(unregister_dataset(name))
        assert(!registered_dataset?(name))
      end
    end
  end
end
