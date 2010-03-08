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
    $MEMOS = {}
    $ASYNC_MEMOS = {}

    # Clears the memoization data.
    def System.clear_memos
      $MEMOS.clear
      $ASYNC_MEMOS.clear
    end

    # Stores the memoization data to file filename.
    def System.store_memos filename
      File.open(filename, 'w') do |file|
        Marshal.dump([$MEMOS, $ASYNC_MEMOS], file)
      end
    end

    # Restores the memoization data to file filename.
    def System.restore_memos filename
      File.open(filename, 'r') do |file|
        $MEMOS, $ASYNC_MEMOS = Marshal.load(file)
      end
    end

    # Returns the memoization data for function fname.
    def System.get_memos fname
      ensure_memos $MEMOS, fname
    end

    # Returns the memoization data for function fname.
    def System.get_async_memos fname
      ensure_memos $ASYNC_MEMOS, fname
    end

    # Returns true if the outcome of one or more asynchronous tasks
    # that have been started is still unknown.
    def System.dirty_async?
      $ASYNC_MEMOS.reject do |fname, memos|
        memos.detect do |fn_args, run_state|
          started, result = run_state
          started && ! result
        end
      end
    end

    # Purges the memoization data for function fname where
    # asynchronous tasks have been started, but are not complete.
    def System.purge_async_failed fname # :nodoc
      memos = get_async_memos fname
      memos.reject! do |fn_args, run_state|
        started, result = run_state
        started && ! result
      end
    end

    private
    def System.ensure_memos hash, key # :nodoc
      if hash.has_key? key
        hash[key]
      else
        hash[key] = {}
      end
    end
  end
end
