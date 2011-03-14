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

module Percolate
  def true_task work_dir = '.'
    task([work_dir], cd(work_dir, 'true'),
         :pre => lambda { work_dir },
         :post => lambda { true },
         :result => lambda { true })
  end

  # A task which always fails.
  def false_task work_dir = '.'
    task([work_dir], cd(work_dir, 'false'),
         :pre => lambda { true },
         :post => lambda { true },
         :result => lambda { false })
  end
end
