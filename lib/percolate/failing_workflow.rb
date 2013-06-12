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

module Percolate
  # The failing workflow. This fails by running the Unix 'false'
  # command.
  class FailingWorkflow < Workflow
    description <<-DESC
The failing workflow. This fails by running the Unix 'false' command.
    DESC

    usage <<-USAGE
FailingWorkflow *args

Arguments:

- args (Array): args are ignored

Returns:

- true
    USAGE

    version VERSION

    def run(*args)
      # args ignored intentionally
      false_task
    end
  end
end
