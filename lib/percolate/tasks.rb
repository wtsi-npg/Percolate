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
  include Percolate::Asynchronous

  # A task which succeeds.
  def true_task work_dir = '.', env = {}
    task :true_task, [work_dir], Percolate.cd(work_dir, 'true'), env,
         :having   => lambda { work_dir },
         :confirm  => lambda { true },
         :yielding => lambda { true }
  end

  # A task which always fails.
  def false_task work_dir = '.', env = {}
    task :false_task, [work_dir], Percolate.cd(work_dir, 'false'), env,
         :having   => lambda { true },
         :confirm  => lambda { true },
         :yielding => lambda { false }
  end

  def rsync_file source_host, source_path, dest_file, work_dir, log, env = {}
    dest = File.join work_dir, dest_file
    command = "rsync -azL #{source_host}:#{source_path} #{dest}"

    lsf_task :rsync_file, [source_host, source_path, dest_file, work_dir],
             lsf(:rsync_file, $$, Percolate.cd(work_dir, command), log), env,
             :having   => lambda { source_host && source_path &&
                               dest_file && work_dir },
             :confirm  => lambda { lsf_run_success?(log) && FileTest.exists?(dest) },
             :yielding => lambda { dest }
  end
end
