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
  # A task that should always succeed. It executes the Unix 'true' command.
  #
  # Arguments:
  #
  # - work_dir (String): The working directory. Optional, defaults to '.'
  #
  # Returns:
  #
  # - true.
  def true_task work_dir = '.'
    task([work_dir], cd(work_dir, 'true'),
         :pre => lambda { work_dir },
         :result => lambda { true })
  end

  # A task that should always fail. It executes the Unix 'false' command.
  #
  # Arguments:
  #
  # - work_dir (String): The working directory. Optional, defaults to '.'
  #
  # Returns:
  #
  # - false.
  def false_task work_dir = '.'
    task([work_dir], cd(work_dir, 'false'),
         :pre => lambda { work_dir },
         :result => lambda { false })
  end
end
