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
  class TaskWrapper
    attr_accessor :message_host
    attr_accessor :message_port
    attr_accessor :message_queue
    attr_accessor :async_wrapper

    # Initializes a new Asynchronizer
    #
    # Arguments (keys and values):
    #  - :message_host (String): The message queue host. Optional, defaults to
    #    'localhost'.
    #  - :message_port (integer): The message queue port. Optional, defaults to
    #    11300.
    #  - :async_wrapper (String): The executable Percolate wrapper that runs
    #    the command and calls back to the message queue. Optional, defaults to
    #    'percolate-wrap'.
    def initialize(args = {})
      defaults = {:message_host => 'localhost',
                  :message_port => 11300,
                  :async_wrapper => 'percolate-wrap'}
      args = defaults.merge(args)

      @message_host = args[:message_host]
      @message_port = args[:message_port]
      @message_queue = nil
      @async_wrapper = args[:async_wrapper]
    end

    # Returns a new message queue client instance.
    #
    # Returns:
    #
    #  - A MessageClient.
    def message_client
      Percolate.log.debug("Connecting to message host #{self.message_host} " +
                              "port #{self.message_port}")
      MessageClient.new(self.message_queue, self.message_host, self.message_port)
    end

    protected
    def command_string(task_id)
      "#{self.async_wrapper} --host #{self.message_host} " +
          "--port #{self.message_port} " +
          "--queue #{self.message_queue} " +
          "--task #{task_id}"
    end
  end
end
