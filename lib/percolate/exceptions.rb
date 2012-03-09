#--
#
# Copyright (c) 2012 Genome Research Ltd. All rights reserved.
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

  # An error raised by Percolate.
  class PercolateError < StandardError
  end

  # An error raised by Percolate outside the context of a running Workflow
  class CoreError < PercolateError
  end

  # An error raised while accessing a workflow definition
  class DefinitionError < CoreError
    attr_reader :definition

    def initialize(msg, definition)
      super(msg)
      @definition = definition
    end
  end

  # An error raised in the context of a Percolate Workflow.
  class WorkflowError < PercolateError
    attr_reader :workflow

    def initialize(msg, args = {})
      super(msg)
      @workflow = args[:workflow]
    end
  end

  # An error raised by a Percolate task.
  class TaskError < WorkflowError
    attr_reader :task

    def initialize(msg, args = {})
      super
      @task = args[:task]
    end
  end

  # An error raised when invalid arguments are passed to a task method
  class TaskArgumentError < TaskError
    attr_reader :argument
    attr_reader :value

    def initialize(msg, args = {})
      super
      @argument = args[:argument]
      @value = args[:value]
    end
  end

  # An error raised by an asynchronous Percolate task.
  class AsyncTaskError < TaskError
  end

end
