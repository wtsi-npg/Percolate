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
  module Utilities
    # Returns a copy of String command with a change directory operation
    # prefixed.
    def cd(path, command)
      "cd #{path} \; #{command}"
    end

    def system_command(command)
      out = []
      IO.popen(command) { |io| out = io.readlines }
      [$?, out]
    end

    def command_success?(process_status)
      process_status.exited? && process_status.exitstatus.zero?
    end
  end
end
