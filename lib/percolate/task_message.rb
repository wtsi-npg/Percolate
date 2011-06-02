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

module Percolate
  # A message describing a change in task state, to be sent via Beanstalk
  # message queue.
  class TaskMessage
    TASK_STATES = [:started, :finished]

    attr_reader :task_identity, :command, :state, :exit_code, :time

    def initialize(task_identity, command, state, exit_code = nil,
                   time = Time.now)
      unless TASK_STATES.include?(state)
        raise ArgumentError,
              "Invalid state argument #{state}, must be one of " +
                  TASK_STATES.inspect
      end

      @task_identity, @command, @state, @exit_code, @time =
          task_identity, command, state, exit_code, time
    end
  end
end
