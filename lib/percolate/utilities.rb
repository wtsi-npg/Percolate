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

module Percolate
  module Utilities
    # Returns the class constant for a fully qualified class name string such as
    # 'HTS::Workflows::PairedAlignment'.
    def find_class(string)
      string.split('::').inject(Kernel) { |scope, name| scope.const_get(name) }
    end

    # Returns a copy of String command with a change directory operation
    # prefixed.
    def cd(path, command)
      "cd #{path} \; #{command}"
    end

    # Returns an absolute path to file. If file is a relative path, it is
    # taken to be relative to dir.
    def absolute_path(file, dir)
      if File.dirname(file) == '.'
        File.expand_path(File.join(dir, file))
      else
        File.expand_path(file)
      end
    end

    def absolute_path?(path)
      path && path.start_with?('/')
    end

    def storage_root(storage_location)
      File.join(storage_location, 'sanger', ENV['USER'])
    end

    def abstract_path?(path)
      !absolute_path?(path) && path.respond_to?(:metadata) &&
          (path.metadata.has_key?(:storage_location) ||
              path.metadata.has_key?(:dataset))
    end

    def concrete_path(abstract_path)
      if abstract_path?(abstract_path)
        meta = abstract_path.metadata
        root =
            case
              when meta[:storage_location] && !meta[:dataset]
                storage_root(meta[:storage_location])
              when meta[:dataset] && !meta[:storage_location]
                storage_root(meta[:dataset])
              else
                raise CoreError,
                      "Invalid abstract path '#{abstract_path}': " +
                          "both :storage_location and :dataset were supplied: " +
                          meta.inspect
            end

        concrete = File.join(root, abstract_path)
        concrete.extend(Metadata)
        concrete.metadata = abstract_path.metadata
        concrete
      else
        raise ArgumentError, "#{abstract_path} is not an abstract path"
      end
    end

    # Tests each element of Array files to check that it is a readable,
    # regular file. If any element does not designate such a file, a
    # PercolateTaskError is raised. Optionally, the caller may choose to
    # avoid raising the error by setting the :error argument to false, in
    # which case a copy of the files Array is returned, with any such elements
    # replaced by nil.
    def ensure_files(files, args = {})
      err = lambda { |file, msg| raise TaskError.new("File '#{file}' #{msg}", args) }

      defaults = {:error => true}
      args = defaults.merge(args)
      error = args[:error]
      found = []

      files.each do |file|
        case
          when !FileTest.exist?(file)
            error && err.call(file, 'does not exist')
          when !FileTest.file?(file)
            error && err.call(file, 'is not a regular file')
          when !FileTest.readable?(file)
            error && err.call(file, 'is not readable')
          else
            found << file
        end
      end

      Percolate.log.debug("Expected #{files.inspect}, found #{found.inspect}")

      if files == found
        files
      end
    end

    # Returns true if the Array args contains no nil values after being
    # flattened.
    def args_available?(*args)
      args.flatten.all?
    end

    # Returns a CLI argument String created by prefixing each element in args
    # and then joining with sep.
    #
    # Example:
    #
    # cli_arg_cat(["x", 1, "y", 2])
    #  => "-x 1 -y 2"
    def cli_arg_cat(args, prefix = '-', sep = ' ')
      args.collect { |arg| prefix + arg.to_s }.join(sep)
    end

    # Returns an Array of CLI argument strings created by joining each
    # pair in Hash args with sep. If block &key is supplied, it will be used
    # to transform the stringified key before joining. For boolean arguments,
    # the key is returned if the value is true, otherwise the pair is removed.
    #
    # Example:
    #
    # cli_arg_map({:x => 1, :y => 2, :a => true, :b => false}, :prefix => '-')
    #  => ["-x 1", "-y 2", "-a"]
    #
    # cli_arg_map({:x_y => 1, :a => true}, :prefix => '--', :sep => '=') { |key|
    #   key.gsub(/_/, '-') }
    #  => ["--x-y=1", "--a"]
    def cli_arg_map(map, args = {}, &key)
      defaults = {:prefix => '', :sep => ' '}
      args = defaults.merge(args)

      map.collect { |pair|
        case pair[1]
          when NilClass, FalseClass
            nil
          when TrueClass
            str = pair[0].to_s
            args[:prefix] + (key && key.call(str) || str)
          else
            strs = pair.collect { |elt| elt.to_s }
            key && strs[0] = key.call(strs[0])
            args[:prefix] + strs.join(args[:sep])
        end
      }.compact
    end

    def system_command(command)
      # A bug in Ruby popen3 means that it does not return the correct exit
      # code; it always returns 0.
      out = []
      IO.popen(command) { |io| out = io.readlines }
      [$?, out]
    end

    def command_success?(process_status)
      process_status.exited? && process_status.exitstatus.zero?
    end
  end
end
