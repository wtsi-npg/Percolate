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

# This module provides support for data-aware scheduling extension to LSF, used
# at the WTSI. Anyone else can safely ignore it.

module Percolate
  module LSFDataAware
    # Returns the name of the LSF datactrl executable.
    def datactrl
      @datactrl || 'datactrl'
    end

    # Sets the name of the LSF datactrl executable.
    def datactrl=(program)
      @datactrl = program
    end

    # Returns true if the LSF datactrl executable is available.
    def datactrl_available?()
      system("which #{self.datactrl} >/dev/null 2>&1")
    end

    # Returns a Hash of the datasets that have been registered by the current
    # process, the keys being dataset names and the values their corresponding
    # locations. The contents of the Hash are not persistent across Percolate
    # invocations.
    def registered_datasets()
      @datasets ||= {}
    end

    # Registers an LSF dataset using datactrl and returns the dataset name on
    # success.
    def register_dataset(name, location)
      unless name
        raise ArgumentError, "A dataset name argument is required"
      end
      unless location
        raise ArgumentError, "A dataset location argument is required"
      end

      command = "#{self.datactrl} dataset reg -k #{location} #{name}"
      status, stdout = system_command(command)
      success = command_success?(status)

      if success
        registered_datasets[name] = location
        name
      else
        raise PercolateError,
              "Failed to register dataset '#{name}' at '#{location}'"
      end
    end

    # Unregisters a named LSF dataset using datactrl and returns true on
    # success. Raises errors if the dataset is not registered or if datactrl
    # fails.
    def unregister_dataset(name)
      unless name
        raise ArgumentError, "A dataset name argument is required"
      end

      unless registered_dataset?(name)
        raise ArgumentError, "Dataset '#{name}' is not registered"
      end

      command = "#{self.datactrl} dataset unreg #{name}"
      status, stdout = system_command(command)
      success = command_success?(status)

      if success
        registered_datasets.delete(name)
        success
      else
        raise PercolateError,
              "Failed to unregister dataset #{name}"
      end
    end

    # Returns true if a named LSF dataset is registered.
    def registered_dataset?(name)
      unless name
        raise ArgumentError, "A dataset name argument is required"
      end

      registered_datasets.has_key?(name)
    end
  end
end
