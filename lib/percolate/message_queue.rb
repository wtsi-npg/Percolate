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
    @@message_host = 'hgs3b' # 'localhost'
    @@message_port = '11300'
    @@message_queue = nil

    def Asynchronous.message_host
      @@message_host
    end

    def Asynchronous.message_port
      @@message_port
    end

    def Asynchronous.message_queue name = nil
      if name
        $log.debug("Setting message queue to #{name}")
        @@message_queue = name
      end

      @@message_queue
    end

    class MessageClientArguments < Hash
      def initialize args
        super

        opts = OptionParser.new do |opts|
          opts.banner = "Usage: #$0 [options]"
          t = [:task_id, '-t', '--task task_id',  'Percolate task identity']
          q = [:queue,   '-q', '--queue queue',   'Percolate queue name']
          h = [:host,    '-h', '--host hostname', 'Percolate queue host']
          p = [:port,    '-p', '--port port',     'Percolate queue port']
          [t, q, h, p].each do |key, short, long, doc|
            opts.on(short, long, doc) { |opt| self[key] = opt }
          end

          opts.on('-?', '--help', 'Display this help and exit') do
            $stderr.puts opts
            exit
          end
        end

        begin
          opts.parse!(args)

        rescue OptionParser::ParseError => pe
          $stderr.puts opts
          $stderr.puts "\nInvalid argument: #{pe}"
        end

        self
      end
    end

    class MessageClient
      DEFAULT_HOST = 'localhost'
      DEFAULT_PORT = '11300'

      attr_reader :host, :port, :pool

      def initialize queue, host = DEFAULT_HOST, port = DEFAULT_PORT
        @host, @port = host, port
        @pool = Beanstalk::Pool.new("#{self.host}:#{self.port}")
        pool.watch(queue)
        pool.use(queue)
        pool.ignore('default')
      end

      def send_message message
        self.pool.yput message
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

      attr_reader :task_identity, :state, :exit_code, :time

      def initialize task_identity, state, exit_code = nil, time = Time.now
        unless TASK_STATES.include?(state)
          raise ArgumentError,
                "Invalid state argument #{state}, must be one of " <<
                TASK_STATES.inspect
        end

        @task_identity, @state, @exit_code, @time =
          task_identity, state, exit_code, time
      end
    end

    def Asynchronous.message_client
      $log.debug("Connecting to message host #{message_host} " <<
                 "port #{message_port}")
      MessageClient.new(message_queue, message_host, message_port)
    end
  end
end
