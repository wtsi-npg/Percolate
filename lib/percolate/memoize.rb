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
  class Memoizer
    attr_accessor :memos
    attr_accessor :async_memos

    def initialize
      @memos = {}
      @async_memos = {}
    end

    def clear_memos
      self.memos.clear
      self.async_memos.clear
    end

    def store_memos place, state
      File.open(place, 'w') { |file|
        Marshal.dump({:percolate_version => Percolate::VERSION,
                      :workflow_state => state,
                      :memos => self.memos,
                      :async_memos => self.async_memos}, file)
      }
    end

    def restore_memos place
      restored = File.open(place, 'r') { |file|
        ensure_valid_memos(place, Marshal.load(file))
      }

      self.memos = restored[:memos]
      self.async_memos = restored[:async_memos]
      restored[:workflow_state]
    end

    def method_memos key
      ensure_memos(self.memos, key)
    end

    def async_method_memos key
      ensure_memos(self.async_memos, key)
    end

    # Updates memoization results for asynchronous tasks by polling a
    # the current message queue. Returns true if any messages were
    # received, or false otherwise.
    def update_async_memos
      client = Asynchronous.message_client
      log = Percolate.log
      updates = Hash.new

      log.debug("Started fetching messages from #{client.inspect}")

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
                "#{Asynchronous.message_queue}")
      updates.each_value { |msgs|
        msgs.each { |msg| log.debug("Received #{msg.inspect}") }
      }

      self.async_memos.each { |fname, memos|
        memos.each { |fn_args, result|
          unless result.finished?
            log.debug("Checking messages for updates to #{result.inspect}")

            task_id = result.task_identity
            if updates.has_key?(task_id)
              msgs = updates[task_id]
              msgs.each { |msg|
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

      client.close

      updates.size > 0
    end

    def async_run_finished? key, args
      result = self.async_method_memos(key)[args]
      result && result.finished?
    end

    # Returns true if the outcome of one or more asynchronous tasks
    # that have been started is still unknown.
    def dirty_async?
      dirty = self.async_memos.keys.select { |key| self.dirty_async_memos?(key) }
      !dirty.empty?
    end

    protected
    # Removes memoized values for failed asynchronous tasks so that
    # they may be run again
    def purge_async_memos
      log = Percolate.log
      log.debug("Purging failed asynchronous tasks")
      log.debug("Before purging: #{self.async_memos.inspect}")

      purged = Hash.new

      self.async_memos.each_pair { |key, memos|
        purged[key] = memos.reject { |fn_args, result|
          result && result.failed?
        }

        log.debug("After purging: #{purged.inspect}")
      }

      self.async_memos = purged
    end

    def dirty_async_memos? key
      memos = self.async_method_memos(key)
      dirty = memos.reject { |fn_args, result|
        result && result.submitted? && result.finished?
      }

      !dirty.keys.empty?
    end

    private
    def ensure_valid_memos place, memos
      msg = "Memoization data restored from '#{place}' is invalid"

      case
        when !memos.is_a?(Hash)
          raise PercolateError, msg + ": not a Hash"
        when !memos.key?(:percolate_version)
          raise PercolateError, msg + ": no Percolate version was stored"
        when !memos[:percolate_version] == Percolate::VERSION
          raise PercolateError, msg +
          ": Percolate version of memos #{memos[:percolate_version]} " +
          "does not match current the version #{Percolate.VERSION}"
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

    def ensure_memos hash, key # :nodoc
      if hash.has_key?(key)
        hash[key]
      else
        hash[key] = {}
      end
    end
  end
end
