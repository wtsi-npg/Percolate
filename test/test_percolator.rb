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

require 'fileutils'
require 'tmpdir'
require 'uri'
require 'yaml'
require 'test/unit'

libpath = File.expand_path('../lib')
$:.unshift(libpath) unless $:.include?(libpath)

require 'percolate'

module TestPercolate
  class TestPercolator < Test::Unit::TestCase
    include Percolate

    def setup
      super
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
      open(File.join data_path, 'percolate_config.yml') { |file|
        config = YAML.load(file)

        assert_equal('test', config['root_dir'])
        assert_equal('test-percolate.log', config['log_filename'])
        assert_equal('INFO', config['log_level'])
      }
    end

    def test_percolator_arguments
      # assert(PercolatorArguments.new(['-h']))
      # assert(PercolatorArguments.new(['-v']))
      # assert(PercolatorArguments.new(['-w']))

      args = PercolatorArguments.new(['-p'])
      assert_equal({:percolate => true}, args)
    end

    def test_new_percolator
      percolator = Percolator.new({ 'root_dir'  => data_path(),
                                    'log_file'  => 'percolate-test.log',
                                    'log_level' => 'DEBUG',
                                    'msg_host'  => 'hgs3b',
                                    'msg_port'  => 11301 })
      assert_equal(data_path, percolator.root_dir)

      assert_raise ArgumentError do
        Percolator.new({'log_file' => '/'})
      end
    end

    def test_find_definitions
      percolator = Percolator.new({ 'root_dir'  => data_path(),
                                    'log_file'  => 'percolate-test.log',
                                    'log_level' => 'DEBUG',
                                    'msg_host'  => 'hgs3b',
                                    'msg_port'  => 11301 })
      assert_equal(['test_def1.yml', 'test_def2.yml'],
                   percolator.find_definitions.sort.collect { |file|
                     File.basename(file)
                   })
    end

    def test_find_run_files
      percolator = Percolator.new({ 'root_dir'  => data_path(),
                                    'log_file'  => 'percolate-test.log',
                                    'log_level' => 'DEBUG',
                                    'msg_host'  => 'hgs3b',
                                    'msg_port'  => 11301 })
      assert_equal(['test_def1.run'],
                   percolator.find_run_files.collect { |file|
                     File.basename(file)
                   })
    end

    def test_find_new_definitions
      percolator = Percolator.new({ 'root_dir'  => data_path(),
                                    'log_file'  => 'percolate-test.log',
                                    'log_level' => 'DEBUG',
                                    'msg_host'  => 'hgs3b',
                                    'msg_port'  => 11301 })
      assert_equal(['test_def2.yml'],
                   percolator.find_new_definitions.collect { |file|
                     File.basename(file)
                   })
    end

    def test_read_definition
      percolator = Percolator.new({ 'root_dir'  => data_path(),
                                    'log_file'  => 'percolate-test.log',
                                    'log_level' => 'DEBUG',
                                    'msg_host'  => 'hgs3b',
                                    'msg_port'  => 11301 })
      defn1 = percolator.read_definition(File.join(percolator.run_dir,
                                                   'test_def1.yml'))
      defn2 = percolator.read_definition(File.join(percolator.run_dir,
                                                   'test_def2.yml'))

      assert defn1.is_a?(Array)
      assert_equal(EmptyWorkflow, defn1[0])
      assert_equal(['/tmp'], defn1[1])

      assert defn2.is_a? Array
      assert_equal(FailingWorkflow, defn2[0])
      assert_equal(['/tmp'], defn2[1])

      assert_raise PercolateError do
        percolator.read_definition('no_such_file_exists')
      end

      assert_raise PercolateError do
        percolator.read_definition(data_path) # A directory, not a file
      end

      assert_raise PercolateError do
        percolator.read_definition(File.join(data_path,
                                             'bad_module_def.yml'))
      end

      assert_raise PercolateError do
        percolator.read_definition(File.join(data_path,
                                             'bad_workflow_def.yml'))
      end

      assert_raise PercolateError do
        percolator.read_definition(File.join(data_path,
                                             'no_module_def.yml'))
      end

      assert_raise PercolateError do
      percolator.read_definition(File.join(data_path,
                                           'no_workflow_def.yml'))
      end
    end

    def test_percolate_tasks_pass
      begin
        percolator = Percolator.new({ 'root_dir'  => data_path(),
                                      'log_file'  => 'percolate-test.log',
                                      'log_level' => 'DEBUG',
                                      'msg_host'  => 'hgs3b',
                                      'msg_port'  => 11301 })
        def_file = File.join(percolator.run_dir, 'test_def1_tmp.yml')
        run_file = File.join(percolator.run_dir, 'test_def1_tmp.run')

        FileUtils.cp(File.join(percolator.run_dir, 'test_def1.yml'), def_file)
        assert(percolator.percolate_tasks(def_file).passed?)

        [def_file, run_file].each { |file|
          assert(File.exists?(File.join(percolator.pass_dir,
                                        File.basename(file))))
        }
      ensure
        [def_file, run_file].each { |file|
          File.delete(File.join(percolator.pass_dir, File.basename(file)))
        }
      end
    end

    def test_percolate_tasks_fail
      begin
        percolator = Percolator.new({ 'root_dir'  => data_path(),
                                      'log_file'  => 'percolate-test.log',
                                      'log_level' => 'DEBUG',
                                      'msg_host'  => 'hgs3b',
                                      'msg_port'  => 11301 })
        def_file = File.join(percolator.run_dir, 'test_def2_tmp.yml')
        run_file = File.join(percolator.run_dir, 'test_def2_tmp.run')

        FileUtils.cp(File.join(percolator.run_dir, 'test_def2.yml'), def_file)
        assert(percolator.percolate_tasks(def_file).failed?)

        [def_file, run_file].each { |file|
          assert(File.exists?(File.join(percolator.fail_dir,
                                        File.basename(file))))
        }
      ensure
        [def_file, run_file].each { |file|
          File.delete(File.join(percolator.fail_dir, File.basename(file)))
        }
      end
    end

    def test_substitute_uris
      percolator = Percolator.new({ 'root_dir'  => data_path(),
                                    'log_file'  => 'percolate-test.log',
                                    'log_level' => 'DEBUG',
                                    'msg_host'  => 'hgs3b',
                                    'msg_port'  => 11301 })
      assert_equal([1, 1.0, 'foo'], percolator.substitute_uris([1, 1.0, 'foo']))

      assert(percolator.substitute_uris(['file:///foo']).first.is_a?(URI))
      assert(percolator.substitute_uris(['urn:foo:bar']).first.is_a?(URI))
    end
  end
end
