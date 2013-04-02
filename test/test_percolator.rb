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

require 'rubygems'
require 'fileutils'
require 'tmpdir'
require 'uri'
require 'yaml'
require 'test/unit'

devpath = File.expand_path(File.join(File.dirname(__FILE__), '..'))
libpath = File.join(devpath, 'lib')
testpath = File.join(devpath, 'test')

$:.unshift(libpath) unless $:.include?(libpath)
require 'percolate'
require File.join(testpath, 'helper')

module TestPercolate
  class TestPercolator < Test::Unit::TestCase
    include Percolate
    include Helper

    def initialize(name)
      super(name)
      @msg_host = 'localhost'
      @msg_port = 11300
    end

    def setup
      super
      Percolate.log = Logger.new(File.join(data_path, 'test_percolator.log'))
    end

    def teardown
      super
    end

    def data_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'data'))
    end

    def test_path
      File.expand_path(File.join(File.dirname(__FILE__), '..', 'test'))
    end

    def test_read_config
      open(File.join data_path, 'percolate_config.yml') do |file|
        config = YAML.load(file)

        assert_equal('test', config['root_dir'])
        assert_equal('test-percolate.log', config['log_filename'])
        assert_equal('INFO', config['log_level'])
        assert_equal(2, config['max_processes'])
      end
    end

    def test_percolator_arguments
      # assert(PercolatorArguments.new(['-h']))
      # assert(PercolatorArguments.new(['-v']))
      # assert(PercolatorArguments.new(['-w']))

      args = PercolatorArguments.new(['-p'])
      assert_equal({:percolate => true}, args)
    end

    def test_new_percolator
      percolator = Percolator.new({'root_dir' => data_path(),
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      assert_equal(data_path, percolator.root_dir)

      assert_raise ArgumentError do
        Percolator.new({'log_file' => '/'})
      end
    end

    def test_find_definitions
      percolator = Percolator.new({'root_dir' => data_path(),
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      assert_equal(['test_def1.yml', 'test_def2.yml'],
                   percolator.find_definitions.sort.collect { |file|
                     File.basename(file)
                   })
    end

    def test_find_run_files
      percolator = Percolator.new({'root_dir' => data_path(),
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      assert_equal(['test_def1.run'],
                   percolator.find_run_files.collect { |file|
                     File.basename(file)
                   })
    end

    def test_find_new_definitions
      percolator = Percolator.new({'root_dir' => data_path(),
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      assert_equal(['test_def2.yml'],
                   percolator.find_new_definitions.collect { |file|
                     File.basename(file)
                   })
    end

    def test_read_definition
      percolator = Percolator.new({'root_dir' => data_path(),
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      defn1 = percolator.read_definition(File.join(percolator.run_dir,
                                                   'test_def1.yml'))
      defn2 = percolator.read_definition(File.join(percolator.run_dir,
                                                   'test_def2.yml'))

      assert(defn1.is_a?(Array))
      assert_equal(EmptyWorkflow, defn1[0])
      assert_equal(['/tmp'], defn1[1])

      assert(defn2.is_a?(Array))
      assert_equal(FailingWorkflow, defn2[0])
      assert_equal(['/tmp'], defn2[1])

      assert_raise DefinitionError do
        percolator.read_definition('no_such_file_exists')
      end

      assert_raise DefinitionError do
        percolator.read_definition(data_path) # A directory, not a file
      end

      assert_raise DefinitionError do
        percolator.read_definition(File.join(data_path, 'bad_module_def.yml'))
      end

      assert_raise DefinitionError do
        percolator.read_definition(File.join(data_path, 'bad_workflow_def.yml'))
      end

      assert_raise DefinitionError do
        percolator.read_definition(File.join(data_path, 'no_module_def.yml'))
      end

      assert_raise DefinitionError do
        percolator.read_definition(File.join(data_path, 'no_workflow_def.yml'))
      end
    end

    def test_percolate_tasks_pass
      work_dir = make_work_dir('test_percolate_tasks_pass', data_path)
      percolator = Percolator.new({'root_dir' => work_dir,
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})
      def_file = File.join(percolator.run_dir, 'test_def1_tmp.yml')
      run_file = File.join(percolator.run_dir, 'test_def1_tmp.run')

      FileUtils.cp(File.join(data_path, 'in', 'test_def1.yml'), def_file)
      assert(percolator.percolate_tasks(def_file).passed?)

      [def_file, run_file].each do |file|
        assert(File.exists?(File.join(percolator.pass_dir,
                                      File.basename(file))))
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end

    def test_percolate_tasks_fail
      work_dir = make_work_dir('test_percolate_tasks_fail', data_path)
      percolator = Percolator.new({'root_dir' => work_dir,
                                   'log_file' => 'percolate-test.log',
                                   'log_level' => 'INFO',
                                   'msg_host' => @msg_host,
                                   'msg_port' => @msg_port})

      def_file = File.join(percolator.run_dir, 'test_def2_tmp.yml')
      run_file = File.join(percolator.run_dir, 'test_def2_tmp.run')

      FileUtils.cp(File.join(data_path, 'in', 'test_def2.yml'), def_file)
      assert(percolator.percolate_tasks(def_file).failed?)

      [def_file, run_file].each do |file|
        assert(File.exists?(File.join(percolator.fail_dir, File.basename(file))))
      end

      Percolate.log.close
      remove_work_dir(work_dir)
    end
  end
end
