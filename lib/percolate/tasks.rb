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

  ## A task which succeeds.
  def true_task work_dir = '.', env = {}
    task :true_task, [work_dir], cd(work_dir, 'true'), env,
         :having   => lambda { work_dir },
         :confirm  => lambda { true },
         :yielding => lambda { true }
  end

  ## A task which always fails.
  def false_task work_dir = '.', env = {}
    task :false_task, [work_dir], cd(work_dir, 'false'), env,
         :having   => lambda { true },
         :confirm  => lambda { true },
         :yielding => lambda { false }
  end

  ## Ruby has mkdir already. This is only an example of a task.
  def mkdir path, work_dir = '.', log = nil, env = {}
    dir = File.join work_dir, path

    task :mkdir, [path, work_dir], cd(work_dir, "mkdir #{path}"), env,
         :having   => lambda { path and work_dir },
         :confirm  => lambda { FileTest.directory?(dir) },
         :yielding => lambda { dir }
  end

  def copy_file source_path, dest_file, work_dir = '.', log = nil, env = {}
    dest = File.join work_dir, dest_file

    task :copy_file, [source_path, dest_file, work_dir],
         cd(work_dir, "cp #{source_path} #{dest}"), env,
         :having   => lambda { source_path and work_dir },
         :confirm  => lambda { FileTest.exists?(dest) },
         :yielding => lambda { dest }
  end

  def async_sleep seconds, work_dir, log, env = {}
    command = "sleep #{seconds}"

    lsf_task :async_sleep, [seconds, work_dir],
             lsf(:async_sleep, $$, cd(work_dir, command), log), env,
             :having   => lambda { work_dir },
             :confirm  => lambda { lsf_run_success? log },
             :yielding => lambda { seconds }
  end

  def rsync_file source_host, source_path, dest_file, work_dir, log, env = {}
    dest = File.join work_dir, dest_file
    command = "rsync -azL #{source_host}:#{source_path} #{dest}"

    lsf_task :rsync_file, [source_path, dest_file, work_dir],
             lsf(:rsync_file, $$, cd(work_dir, command), log), env,
             :having   =>  lambda { source_host and source_path and
                                    dest_file and work_dir },
             :confirm  => lambda { lsf_run_success?(log) and FileTest.exists?(dest) },
             :yielding => lambda { dest }
  end
end
