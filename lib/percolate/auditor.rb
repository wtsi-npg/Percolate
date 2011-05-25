#--
#
# Copyright (c) 2011 Genome Research Ltd. All rights reserved.
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

require 'ruport'

module Percolate

  # Simple task summary by run file, sorted by finish time

  # Mean run time of each type of task by run file

  module Auditor
    def mean(numbers)
      numbers.inject(0) { |sum, n| sum += n }.to_f / numbers.size
    end

    def median(numbers)
      if numbers.empty?
        nil
      elsif (numbers.size % 2).zero?
        m = numbers.size / 2
        mean(numbers.sort[m - 1 .. m])
      else
        numbers.sort[numbers.size / 2]
      end
    end

    def variance(numbers)
      if numbers.empty?
        nil
      else
        m = mean(numbers)
        numbers.inject(0) { |var, n| var += (n - m) ** 2 }
      end
    end

    def std_deviation(numbers)
      Math.sqrt(variance(numbers) / (numbers.size - 1))
    end

    # Loads a Percolate run file as a table. Rows are sorted by task finish
    # time, if available.
    #
    # Arguments:
    #
    # - run_file (String): A run file.
    #
    # Returns:
    #
    # - A table.
    def load_run_file(run_file)
      memoizer = Percolate::Memoizer.new
      memoizer.restore_memos!(run_file)

      records = memoizer.results.collect { |result|
        Ruport::Data::Record.new(result.to_a)
      }

      table = Ruport::Data::Table.new(:data => records)
      table.column_names = Result::COLUMN_NAMES

      columns = [:workflow, :run_file]
      defaults = [memoizer.workflow, File.basename(run_file)]

      columns.zip(defaults).each { |c, d|
        table.add_column(c, :position => 0, :default => d)
      }

      now = Time.now
      table.sort_rows_by { |row| row[:finish_time] || now }
    end

    # Loads Percolate run file data as tables.
    #
    # Arguments:
    #
    # - run_files (Array of String): An Array of run files.
    #
    # other arguments (keys and values):
    #  - :merge (boolean): If true, merge all the run file data into a single
    #     table.
    #
    # Returns:
    #
    # - Array of tables.
    def load_run_files(run_files, args = {})
      defaults = {:merge => true}
      args = defaults.merge(args)

      tables = run_files.collect { |f| load_run_file(f) }
      if args[:merge]
        tables.inject() { |t1, t2| t1 + t2 }
      else
        tables
      end
    end

    # Makes a grouping of tables by the values in a given column.
    #
    # Arguments:
    #
    # - table (Object): A table
    #
    # Other arguments (keys and values):
    #
    # - :by (column): Group by the values in column.
    #
    # Returns:
    #
    # - A table group.
    def group(table, args = {})
      Grouping(table, args)
    end

    # Makes a copy of table with the specified columns removed.
    #
    # Arguments:
    #
    # - table (Object): The table to copy.
    # - columns (String or Symbol): The names of the columns to exclude.
    #
    # Returns:
    #
    # - A table.
    def hide_columns(table, *columns)
      table.sub_table(table.column_names - columns)
    end

    # Replaces Date objects in table with formatted date Strings.
    #
    # Arguments :
    #
    # - table (Object): the table to format
    #
    # other arguments (keys and values):
    #  - :format (date format string). The date format. Optional, defaults to
    #     %I:%M:%S %p %d-%m-%Y
    #  - :columns: (Array of columns). Columns within the table containing the
    #     values to be formatted. Optional, defaults to
    #    [:submission_time, :start_time, :finish_time]
    #
    # Returns:
    #
    # - A table.
    def format_dates(table, args = {})
      defaults = {:format => '%I:%M:%S %p %d-%m-%Y',
                  :columns => [:submission_time, :start_time, :finish_time]}
      args = defaults.merge(args)
      format, columns = args[:format], args[:columns]

      t2 = table.dup
      names = t2.column_names
      columns.each { |col|
        if names.include?(col)
          t2.replace_column(col) { |row| row[col] && row[col].strftime(format) }
        end
      }

      t2
    end

    # Given a table containing :task and :run_time columns, groups rows by :task
    # and calculates mean run time per task.
    #
    # Arguments:
    #
    # - table (Object): A table containing :task and :run_time columns.
    #
    # Returns:
    #
    # - A table with :task and :mean_run_time columns.
    def mean_run_time(table)
      mrt = lambda { |g| mean(g.column(:run_time).compact) }

      g_by_task = group(table, :by => :task)
      g_by_task.summary(:task, :mean_run_time => mrt,
                        :order => [:task, :mean_run_time])
    end

    # Writes table to out as text.
    def write_table(table, out = $stdout, width = 1000)
      out.puts(table.as(:text, :table_width => width))
    end

    def audit_run_times(run_files)
      means = load_run_files(run_files, :merge => false).collect { |table|
        hide_columns(table, :task_identity)
      }.collect { |table|
        mean_run_time(table)
      }

      means.zip(run_files).collect { |table, run_file|
        table.add_column(:run_file, :position => 0,
                         :default => File.basename(run_file))
      }.inject() { |t1, t2| t1 + t2 }
    end
  end
end
