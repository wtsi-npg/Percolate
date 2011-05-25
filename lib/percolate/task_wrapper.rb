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
