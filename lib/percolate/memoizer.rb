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
  # A Memoizer is responsible for maintaining mappings of arguments to return
  # values for all Percolate task methods. It allows the mappings to be updated,
  # saved and restored.
  class Memoizer
    # The class of Workflow running cuurently.
    attr_accessor :workflow
    # A mapping of task method names to method memoization tables.
    attr_accessor :memos
    # A mapping of task method names to method memoization tables.
    attr_accessor :async_memos
    attr_accessor :max_processes

    def initialize
      @memos = {}
      @async_memos = {}
      @max_processes = 4
    end

    # Erases all memoization tables.
    def clear_memos!
      Percolate.log.debug("Emptying memo tables")
      self.memos.clear
      self.async_memos.clear
    end

    # Stores memoization data to place along with additional information on the
    # workflow class and its state.
    def store_memos(place, workflow, state)
      File.open(place, 'w') { |file|
        Marshal.dump({:percolate_version => Percolate::VERSION,
                      :workflow => workflow,
                      :workflow_state => state,
                      :memos => self.memos,
                      :async_memos => self.async_memos}, file)
      }
    end

    # Destructively modifies self by reading stored memoization data from place.
    def restore_memos!(place)
      restored = File.open(place, 'r') { |file|
        ensure_valid_memos(place, Marshal.load(file))
      }

      self.workflow = restored[:workflow]
      self.memos = restored[:memos]
      self.async_memos = restored[:async_memos]

      [self.workflow, restored[:workflow_state]]
    end

    # Returns an Array of all the Result objects available.
    def results
      [self.memos, self.async_memos].collect { |memos|
        memos.values.collect { |method_memos| method_memos.values }
      }.flatten
    end

    # Returns the memoization table for the synchronous task method with name
    # key.
    def method_memos(key)
      ensure_memos(self.memos, key)
    end

    # Returns the memoization table for the asynchronous task method with name
    # key.
    def async_method_memos(key)
      ensure_memos(self.async_memos, key)
    end

    # Updates memoization results for asynchronous tasks by polling a
    # the current message queue. Returns true if any messages were
    # received, or false otherwise.
    def update_async_memos!
      client = Percolate.asynchronizer.message_client
      log = Percolate.log
      log.debug("Started fetching messages from #{client.inspect}")

      begin
        client.open_queue

        updates = Hash.new
        loop do
          msg = client.get_message
          if msg
            if updates.has_key?(msg.task_identity)
              updates[msg.task_identity] << msg
            else
              updates[msg.task_identity] = [msg]
            end
          else
            break
          end
        end

        log.debug("Fetched #{updates.size} messages from " +
                  "#{Percolate.asynchronizer.message_queue}")
        updates.each_value { |msgs|
          msgs.each { |msg| log.debug("Received #{msg.inspect}") }
        }

        self.async_memos.values.each { |method_memos|
          method_memos.values.each { |result|
            unless result.finished?
              log.debug("Checking messages for updates to #{result.inspect}")

              task_id = result.task_identity
              if updates.has_key?(task_id)
                updates[task_id].each { |msg|
                  case msg.state
                    when :started
                      if result.started? || result.finished?
                        log.warn("#{task_id} has been restarted")
                      else
                        log.debug("#{task_id} has started")
                      end
                      result.started!(msg.time)
                    when :finished
                      log.debug("#{task_id} has finished")
                      result.finished!(nil, msg.time, msg.exit_code)
                    else
                      raise PercolateError, "Invalid message: " + msg.inspect
                  end
                }
              end
            end
          }
        }
      ensure
        client.close_queue
      end

      updates.size > 0
    end

    # Removes memoized values for failed asynchronous tasks so that
    # they may be run again
    def purge_async_memos!
      log = Percolate.log
      log.debug("Purging failed asynchronous tasks")
      log.debug("Before purging: #{self.async_memos.inspect}")

      purged = Hash.new

      self.async_memos.each_pair { |key, method_memos|
        purged[key] = method_memos.reject { |method_args, result|
          result && result.failed?
        }

        log.debug("After purging #{key}: #{purged.inspect}")
      }

      self.async_memos = purged
    end

    # Returns true if the asynchronous task method with name key, called with
    # arguments margs has finished?
    def async_finished?(key, margs)
      result = self.async_method_memos(key)[margs]
      result && result.finished?
    end

    # Returns true if the outcome of one or more asynchronous tasks
    # that have been started is still unknown.
    def dirty_async?
      !self.async_memos.keys.select { |key| self.dirty_async_memos?(key) }.empty?
    end

    def result_count(&result)
      count_results(self.memos, &result)
    end

    def async_result_count(&result)
      count_results(self.async_memos, &result)
    end

    protected
    def dirty_async_memos?(key) # :nodoc
      !self.async_method_memos(key).values.compact.select { |result|
        result.submitted? && !result.finished?
      }.empty?
    end

    private
    def count_results(memos, &block) # :nodoc
      memos.values.collect { |method_memos|
        results = method_memos.values.compact
        if block
          results.select { |result| yield(result) }.size
        else
          results.size
        end
      }.inject(0) { |n, m| n + m }
    end

    def ensure_valid_memos(place, memos) # :nodoc
      msg = "Memoization data restored from '#{place}' is invalid"

      case
        when !memos.is_a?(Hash)
          raise PercolateError, msg + ": not a Hash"
        when !memos.key?(:percolate_version)
          raise PercolateError, msg + ": no Percolate version was stored"
        when memos[:percolate_version] != Percolate::VERSION
          raise PercolateError, msg +
          ": Percolate version of memos #{memos[:percolate_version]} " +
          "does not match current the version #{Percolate::VERSION}"
        when !memos.key?(:workflow_state)
          raise PercolateError, msg + ": no Workflow state was stored"
        when !Percolate::Workflow::STATES.include?(memos[:workflow_state])
          raise PercolateError, msg + ": workflow state was " +
          "#{memos[:workflow_state]}, expected one of " +
          "#{Percolate::Workflow::STATES}"
        when !memos.key?(:memos) || !memos.key?(:async_memos)
          raise PercolateError, ": memoization data was missing"
      end

      memos
    end

    def ensure_memos(hash, key) # :nodoc
      if hash.has_key?(key)
        hash[key]
      else
        hash[key] = {}
      end
    end
  end
end
