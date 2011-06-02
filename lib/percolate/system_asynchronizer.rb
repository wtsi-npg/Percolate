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
  # An Asynchronizer that runs jobs as simple system calls via fork/exec.
  class SystemAsynchronizer < TaskWrapper
    include Utilities
    include Asynchronizer

    def async_command(task_id, command, work_dir, log, args = {})
      cmd_str = command_string(task_id)
      cd(work_dir, "#{cmd_str} -- #{command}")
    end

    # Makes a system call for a named asynchronous method. The system call
    # executes the command via fork/exec.
    def submit_async(method_name, command)
      unless self.message_queue
        raise PercolateError, "No message queue has been provided"
      end

      process = fork { exec(command) }
      Percolate.log.info("submission reported #{process} for #{method_name}")

      # There seems to be a bug in some versions of Ruby 1.8.7 where detaching the
      # process causes the interpreter to hang. The following line is a workaround.
      # sleep(2)

      Process.detach(process)
    end
  end
end
