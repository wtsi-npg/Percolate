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
  module System
    # Memoization Hash for synchronous tasks
    @@memos = {}
    # Memoization Hash for asynchronous tasks
    @@async_memos = {}

    # Returns a Hash of memoization data for synchronous tasks. Keys
    # are function name symbols, values are Hashes mapping task
    # argument Arrays to Result objects.
    def System.memos
      @@memos
    end

    # Returns a Hash of memoization data for asynchronous tasks. Keys
    # are function name symbols, values are Hashes mapping task
    # argument Arrays to Result objects.
    def System.async_memos
      @@async_memos
    end

    # Clears the memoization data
    def System.clear_memos
      @@memos.clear
      @@async_memos.clear
    end

    # Stores the memoization data to file filename.
    def System.store_memos filename
      File.open(filename, 'w') do |file|
        Marshal.dump([@@memos, @@async_memos], file)
      end
    end

    # Restores the memoization data to file filename.
    def System.restore_memos filename
      File.open(filename, 'r') do |file|
        @@memos, @@async_memos = Marshal.load(file)
      end
    end

    # Returns the memoization data for function fname.
    def System.get_memos fname
      ensure_memos(@@memos, fname)
    end

    # Returns the memoization data for function fname.
    def System.get_async_memos fname
      ensure_memos(@@async_memos, fname)
    end

    # Updates memoization results for asynchronous tasks by polling a
    # the current message queue. Returns true if any messages were
    # received, or false otherwise.
    def System.update_async_memos
      client = Asynchronous.message_client
      updates = Hash.new

      $log.debug("Started fetching messages from #{client.inspect}")

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

      $log.debug("Fetched #{updates.size} messages from " <<
                 "#{Asynchronous.message_queue}")
      updates.each_value do |msgs|
        msgs.each do |msg|
          $log.debug("Received #{msg.inspect}")
        end
      end

      @@async_memos.each do |fname, memos|
        memos.each do |fn_args, result|
          task_id = result.task_identity
          $log.debug("Checking messages for updates to #{result.inspect}")

          if updates.has_key?(task_id)
            msgs = updates[task_id]
            msgs.each do |msg|
              case msg.state
                when :started
                  $log.debug("#{task_id} has started")
                  result.started!(msg.time)
                when :finished
                  $log.debug("#{task_id} has finished")
                  result.finished!(nil, msg.time, msg.exit_code)
              else
                raise PercolateError, "Invalid message: " << msg.inspect
              end
            end
          end
        end
      end

      client.close

      return (updates.size > 0)
    end

    def System.async_run_finished? fname, args
      result = get_async_memos(fname)[args]
      result && result.finished?
    end

    # Returns true if the outcome of one or more asynchronous tasks
    # that have been started is still unknown.
    def System.dirty_async?
      dirty = @@async_memos.keys.select do |fname|
        dirty_async_memos?(fname)
      end

      ! dirty.empty?
    end

    def System.dirty_async_memos? fname
      memos = get_async_memos(fname)
      dirty = memos.reject do |fn_args, result|
        result && result.submitted? && result.finished?
      end

      ! dirty.keys.empty?
    end

    private
    def System.ensure_memos hash, key # :nodoc
      if hash.has_key?(key)
        hash[key]
      else
        hash[key] = {}
      end
    end
  end
end
