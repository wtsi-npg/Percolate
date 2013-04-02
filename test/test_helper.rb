#--
#
# Copyright (c) 2010-2013 Genome Research Ltd. All rights reserved.
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

require 'fileutils'

module TestHelper
  include Percolate

  def test_workflow(name, klass, timeout, path, log, workflow_args, args = {})
    ['in', 'pass', 'fail'].each do |dir|
      root = File.join(path, dir)
      FileUtils.mkdir(root) unless File.exists?(root)
    end

    yml = File.join(path, 'in', "#{name}.yml")
    File.open(yml, 'w') do |out|
      config = {'workflow' => klass.to_s,
                'arguments' => workflow_args}
      out.puts(YAML.dump(config))
    end

    async = args[:async] || 'system'
    max_processes = args[:max_processes] || 2;

    percolator = Percolator.new({'root_dir' => path,
                                 'log_file' => log,
                                 'log_level' => 'DEBUG',
                                 'msg_host' => @msg_host,
                                 'msg_port' => @msg_port,
                                 'async' => async,
                                 'max_processes' => max_processes})

    asynchronizer = Percolate.asynchronizer
    asynchronizer.async_wrapper = File.join(bin_path, 'percolate-wrap')
    asynchronizer.ruby_args = {:I => lib_path}

    # The Percolator returns all its workflows after each iteration
    workflow = nil
    Timeout.timeout(timeout) do
      until workflow && workflow.finished? do
        sleep(15)
        print('#')
        workflow = percolator.percolate.first
      end
    end

    workflow
  end

  def return_available?(value)
    case value
      when Array ; value.all?
      when NilClass ; nil
      else
        true
    end
  end

  def wait_for(name, timeout, interval, &test)
    result = nil

    memoizer = Percolate.memoizer
    asynchronizer = Percolate.asynchronizer
    asynchronizer.message_host = @msg_host
    asynchronizer.message_port = @msg_port
    asynchronizer.message_queue = name + '.' + $$.to_s

    Timeout.timeout(timeout) do
      until return_available?(result) do
        result = test.call
        memoizer.update_async_memos!
        sleep(interval)
        print('#')
      end
    end

    result
  end

  def run_test_if(predicate, msg, &test)
    if predicate.call
      test.call
    else
      $stderr.puts(msg)
    end
  end

  def make_work_dir(name, dir)
    work_dir = File.join(dir, name + '.' + $$.to_s)
    unless File.directory?(work_dir)
      Dir.mkdir(work_dir)
    end

    work_dir
   end

  def remove_work_dir(dir)
    FileUtils.rm_rf(dir)
  end
end
