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
  # Regular expression that matches a partition of a partitioned file.
  PARTITION_REGXEP = Regexp.new('^(.*)\.part\.(\d+)(\.\S+)$')
  # The separator used in creating the names of file partitions.
  PARTITION_SEP = '.'
  # The string used to tag file parts.
  PARTITION_TAG = 'part'

  # Returns separator used in creating the names of file partitions.
  def partition_sep
    PARTITION_SEP
  end

  def partition_tag pref = PARTITION_SEP, post = PARTITION_SEP
    "#{pref}#{PARTITION_TAG}#{post}"
  end

  # Returns an array of n file names that are the partitions of filename, in
  # ascending order.
  def partitions filename, n
    if File.directory?(filename)
      raise ArgumentError,
            "#{filename} could not be partitioned; it is a directory"
    end

    dir = File.dirname(filename)
    if dir == '.'
      dir = ''
    end

    match = File.basename(filename).match(/^(.+)(\.\S+)$/)
    unless match
      raise ArgumentError, "#{filename} could not be partitioned; no suffix"
    end
    prefix = match[1]
    suffix = match[2]

    (0 .. n - 1).collect do |i|
      part = "#{prefix}#{partition_tag}#{i}#{suffix}"

      if dir != ''
        File.join(dir, part)
      else
        part
      end
    end
  end

  # Returns true if filename is a partition.
  def partition? filename
    !parse_partition(filename).nil?
  end

  # Returns the index of filename if it is a partition, or raises an
  # ArgumentError if it is as object other than nil.
  def partition_index filename
    if filename.nil?
      nil
    elsif partition?(filename)
      base, index, suffix = parse_partition(filename)
      index.to_i
    else
      raise ArgumentError, "#{filename} is not a partition"
    end
  end

  # Returns the parent of filename i.e. the file that was partitioned
  # to create filename, or nil if filename is nil.
  def partition_parent filename
    if filename.nil?
      nil
    elsif partition?(filename)
      base, index, suffix = parse_partition(filename)
      "#{base}#{suffix}"
    else
      raise ArgumentError, "#{filename} is not a partition"
    end
  end

  # Returns the template of filename if it is a partition, or raises
  # an ArgumentError if it is not.
  def partition_template filename, placeholder = '%d'
    if partition?(filename)
      replace_partition(filename, placeholder)
    else
      raise ArgumentError, "#{filename} is not a partition"
    end
  end

  # Returns true if Array filenames is not empty and all filenames are
  # distinct and share the same parent i.e. are partitions of the same
  # file.
  def sibling_partitions? filenames
    if (!filenames.empty? && filenames.all? && duplicates(filenames).empty?)
      parents = filenames.collect { |f| partition_parent(f) }
      parents.count(parents.first) == filenames.size
    end
  end

  # Returns true if Array filenames is not empty and all filenames are
  # siblings with indices between 0 and 1- filenames.size, with no
  # duplicates.
  def complete_partitions? filenames
    range = 0...filenames.size
    sibling_partitions?(filenames) &&
        filenames.select { |f| !range.include?(partition_index(f)) }.empty?
  end

  private
  def parse_partition filename
    if PARTITION_REGXEP.match(filename)
      [$1, $2, $3]
    end
  end

  def replace_partition filename, placeholder
    if PARTITION_REGXEP.match(filename)
      "#{$1}#{partition_tag}#{placeholder}#{$3}"
    end
  end

  def duplicates array
    duplicates = Hash.new
    array.each { |elt|
      if duplicates.has_key?(elt)
        duplicates[elt] += 1
      else
        duplicates[elt] = 1
      end
    }

    duplicates.select { |key, value| value > 1 }
  end
end
