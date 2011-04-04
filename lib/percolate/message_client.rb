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

require 'rubygems'
require 'optparse'
require 'beanstalk-client'

module Percolate
  # A Beanstalk client that sends and receives TaskMessages.
  class MessageClient
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = '11300'

    attr_reader :host, :port, :pool

    def initialize queue, host = DEFAULT_HOST, port = DEFAULT_PORT
      @queue, @host, @port = queue, host, port
    end

    def open_queue
      begin
        @pool = Beanstalk::Pool.new(self.host_id)
        pool.watch(@queue)
        pool.use(@queue)
        pool.ignore('default')
      rescue Beanstalk::NotConnected => nc
        raise PercolateError,
              "Failed to connect to message queue server at #{self.host_id} : " +
                  "#{nc.message}"
      end

    end

    def host_id
      "#{self.host}:#{self.port}"
    end

    def send_message message
      self.pool.yput(message)
    end

    def get_message
      if pool.peek_ready
        msg = pool.reserve
        msg_body = msg.ybody
        msg.delete
        msg_body
      end
    end

    def close_queue
      pool && pool.close
    end
  end

  # A message describing a change in task state, to be sent via Beanstalk
  # message queue.
  class TaskMessage
    TASK_STATES = [:started, :finished]

    attr_reader :task_identity, :command, :state, :exit_code, :time

    def initialize task_identity, command, state, exit_code = nil,
        time = Time.now
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
