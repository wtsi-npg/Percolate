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
  class TestWorkflow < Test::Unit::TestCase
    include Percolate

    def data_path
      File.expand_path File.join File.dirname(__FILE__), '..', 'data'
    end

    def test_lsf_run_success?
      assert_nil(lsf_run_success? 'no_such_file')
      assert_nil(lsf_run_success? File.join data_path, 'lsf_incomplete.log')
      assert_equal(false, lsf_run_success?(File.join data_path,
                                                     'lsf_unsuccessful_complete.log'))
      assert(lsf_run_success?(File.join data_path, 'lsf_successful_complete.log'))
    end
  end
end
