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

module Percolate
  # A result of running an external program, including metadata (time
  # started and finished, exit code).
  #
  class Result
    # When a result is presented in tabular form, these are the default column
    # names.
    COLUMN_NAMES = [:task, :mode, :task_identity, :state,
                    :submission_time, :start_time, :finish_time, :run_time,
                    :exit_code]

    # The name of task responsible for the result.
    attr_reader :task
    # The task mode e.g. :native, :sync or :async.
    attr_reader :mode
    # The unique identity of the task instance responsible for the
    # result.
    attr_reader :task_identity

    # The submission time, if available.
    attr_accessor :submission_time
    # The start time, if available.
    attr_accessor :start_time
    # The finish time, if available.
    attr_accessor :finish_time

    # Task return value.
    attr_accessor :value
    # Task exit code.
    attr_accessor :exit_code
    # Task STDOUT.
    attr_accessor :stdout
    # Task STDERR.
    attr_accessor :stderr

    def initialize(task, mode, task_identity, submission_time, start_time = nil,
        finish_time = nil, value = nil, stdout = nil, stderr = nil)
      @task = task
      @mode = mode
      @task_identity = task_identity
      @submission_time = submission_time
      @start_time = start_time
      @finish_time = finish_time
      @value = value
      @stdout = stdout
      @stderr = stderr
    end

    # Sets the Result on completion of a task.
    def finished!(value, finish_time = Time.now, exit_code = 0)
      self.finish_time = finish_time
      self.exit_code = exit_code
      self.value = value
    end

    # Sets the time at which the task started. Tasks may be restarted,
    # in which case the finish time, value, stdout and stderr are set
    # to nil.
    def started!(start_time = Time.now)
      self.start_time = start_time
      self.finish_time = nil
      self.value = nil
      self.stdout = nil
      self.stderr = nil
    end

    # Returns true if the task that will generate the Result's value
    # has been submitted.
    def submitted?
      !self.submission_time.nil?
    end

    # Returns true if the task that will generate the Result's value
    # has been started.
    def started?
      !self.start_time.nil?
    end

    # Returns true if the task that will generate the Result's value
    # has finished.
    def finished?
      !self.finish_time.nil?
    end

    # Returns true if the task that will generate the Result's value
    # has returned something i.e. the value is not nil.
    def value?
      !self.value.nil?
    end

    # Returns true if the task that would have generated the Result's value
    # has failed.
    def failed?
      self.finished? && (self.exit_code.nil? || !self.exit_code.zero?)
    end

    # Returns the run time of the task, if it has finished, otherwise returns
    # nil.
    def run_time
      if started? && finished?
        self.finish_time - self.start_time
      end
    end

    def to_a
      [self.task, self.mode, self.task_identity, state,
       self.submission_time, self.start_time, self.finish_time, self.run_time,
       self.exit_code]
    end

    def to_s
      vstr = self.value.inspect
      if vstr.length > 124
        vstr = vstr[0, 124] + " ..."
      end

      "#<#{self.class} #{self.mode} task_id: #{self.task_identity} #{state} " +
          "sub: #{self.submission_time.inspect} " +
          "start: #{self.start_time.inspect} " +
          "finish: #{self.finish_time.inspect} value: #{vstr}>"
    end

    private
    def state
      case
        when self.finished? && !self.failed?
          :passed
        when self.failed?
          :failed
        when self.started?
          :started
        when self.submitted?
          :submitted
        else
          :pending
      end
    end
  end
end
