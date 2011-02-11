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
  module Asynchronous
    @@message_host = 'localhost'
    @@message_port = '11300'
    @@message_queue = nil

    def Asynchronous.message_host host = nil
      if host
        $log.debug("Setting message host to #{host}")
        @@message_host = host
      end
      @@message_host
    end

    def Asynchronous.message_port port = nil
      if port
        $log.debug("Setting message port to #{port}")
        @@message_port = port
      end

      @@message_port
    end

    def Asynchronous.message_queue name = nil
      if name
        $log.debug("Setting message queue to #{name}")
        @@message_queue = name
      end

      @@message_queue
    end

    def Asynchronous.message_client
      $log.debug("Connecting to message host #{self.message_host} " +
                 "port #{self.message_port}")
      MessageClient.new(self.message_queue, self.message_host,
                        self.message_port)
    end

    class MessageClient
      DEFAULT_HOST = 'localhost'
      DEFAULT_PORT = '11300'

      attr_reader :host, :port, :pool

      def initialize queue, host = DEFAULT_HOST, port = DEFAULT_PORT
        @host, @port = host, port
        @pool = Beanstalk::Pool.new(self.host_id)
        pool.watch(queue)
        pool.use(queue)
        pool.ignore('default')
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

      def close
        pool.close
      end
    end

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
end
