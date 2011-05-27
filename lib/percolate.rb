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

require 'digest'
require 'fileutils'
require 'logger'
require 'uri'

require 'percolate/utilities'
require 'percolate/named_tasks'
require 'percolate/tasks'
require 'percolate/result'
require 'percolate/memoizer'
require 'percolate/message_client'
require 'percolate/task_message'
require 'percolate/task_wrapper'
require 'percolate/asynchronizer'
require 'percolate/system_asynchronizer'
require 'percolate/lsf_asynchronizer'
require 'percolate/workflow'
require 'percolate/empty_workflow'
require 'percolate/failing_workflow'
require 'percolate/percolator'
require 'percolate/auditor'
require 'percolate/partitions'

module Percolate

  VERSION = '0.4.9'

  @log = Logger.new(STDERR)
  @memoizer = Memoizer.new
  @asynchronizer = LSFAsynchronizer.new

  # Exit codes
  # Error in the command line arguments provided
  CLI_ERROR = 10
  # Error in the configuration file
  CONFIG_ERROR = 11
  # Error when the wrapper executed a system call
  WRAPPER_ERROR = 12

  # An error raised by the Percolate system.
  class PercolateError < StandardError ; end

  # An error raised by a Percolate task.
  class PercolateTaskError < PercolateError ; end

  # An error raised by an asynchronous Percolate task.
  class PercolateAsyncTaskError < PercolateTaskError ; end

  class << self
    attr_accessor :log
    attr_accessor :memoizer
    attr_accessor :asynchronizer
  end

  # Returns an array of all Workflow classes, optionally restricting
  # the result to subclasses of ancestor.
  def Percolate.find_workflows(ancestor = Percolate)
    begin
      mod = case ancestor
              when NilClass;
                Percolate
              when String;
                Object.const_get(ancestor)
              when Module;
                ancestor
              else
                raise ArgumentError,
                      "Invalid ancestor argument. Expected a string or " +
                          "constant, but was #{ancestor.inspect}"
            end
    rescue NameError => ne
      raise ArgumentError,
            "Invalid ancestor argument. Expected a Ruby module, " +
                "but was #{ancestor.inspect}"
    end

    ObjectSpace.each_object(Class).select { |c|
      c.ancestors.include?(Workflow) && c.ancestors.include?(mod)
    }
  end
end
