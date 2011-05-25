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
  module Tasks
    include NamedTasks
    # Defines a synchronous external (system call) task method.
    #
    # Arguments:
    #
    # - margs (Array): The arguments that are necessary and sufficient to
    #   identify an invoaction of the task. If the task method is called in
    #   multiple contexts with these same arguments, they will resolve to a
    #   single invocation in the Memoizer.
    # - command (String): A complete command line string to be executed.
    #
    # Other arguments (keys and values):
    #
    # - :pre (Proc): A precondition callback that must evaluate true before
    #   the task is executed. Optional.
    # - :post (Proc): A postcondition callback that must evaluate true before
    #   a result is returned. Optional.
    # - :result (Proc): A return value callback that must evaluate to the desired
    #   return value.
    # - :unwrap (boolean): A boolean indicating that the return value will be
    #   unwrapped.
    #
    #  These Procs may accept none, some, or all the arguments that are
    #  defined by margs. Each will be called with the  appropriate number.
    #  For example, if the :pre Proc has arity 2, it will be called with the
    #  first 2 elements of margs.
    #
    # Returns:
    # - Return value of the :result Proc, or nil.
    def task(margs, command, args = {})
      unwrap = args.delete(:unwrap)
      mname = calling_method
      env = {}

      result = super(mname, margs, command, env, callback_defaults.merge(args))
      maybe_unwrap(result, unwrap)
    end

    # Defines a synchronous native (Ruby) task method.
    #
    # Arguments:
    #
    # - margs (Array): The arguments that are necessary and sufficient to
    #   identify an invocation of the task. If the task method is called in
    #   multiple contexts with these same arguments, they will resolve to a
    #   single invocation in the Memoizer.
    # - command (String): A complete command line string to be executed.
    #
    # Other arguments (keys and values):
    #
    # - :pre (Proc): A precondition callback that must evaluate true before
    #   the task is executed. Optional.
    # - :post (Proc): A postcondition callback that must evaluate true before
    #   a result is returned. Optional.
    # - :result (Proc): A return value callback that must evaluate to the desired
    #   return value.
    # - :unwrap (boolean): A boolean indicating that the return value will be
    #   unwrapped.
    #
    #  These Procs may accept none, some, or all the arguments that are
    #  defined by margs. Each will be called with the  appropriate number.
    #  For example, if the :pre Proc has arity 2, it will be called with the
    #  first 2 elements of margs.
    #
    # Returns:
    # - Return value of the :result Proc, or nil.
    def native_task(margs, args = {})
      mname = calling_method
      defaults = {:pre => lambda { true }}
      args = defaults.merge(args)

      result = super(mname, margs, args[:pre], args[:result])
      maybe_unwrap(result, args[:unwrap])
    end

    # Defines an asynchronous external (batch queue) task method.
    #
    # Arguments:
    #
    # - margs (Array): The arguments that are necessary and sufficient to
    #   identify an invocation of the task. If the task method is called in
    #   multiple contexts with these same arguments, they will resolve to a
    #   single invocation in the Memoizer.
    # - command (String): A complete command line string to be executed.
    # - work_dir (String): The working directory where the batch job will be run.
    # - log (String): The name of the batch queue log file. This may be an
    #   absolute file name, or one relative to the work_dir.
    #
    # Other arguments (keys and values):
    #
    # - :pre (Proc): A precondition callback that must evaluate true before
    #   the task is submitted to the batch queue system. Optional.
    # - :post (Proc): A postcondition callback that must evaluate true before
    #   a result is returned. Optional.
    # - :result (Proc): A return value callback that must evaluate to the desired
    #   return value.
    # - :unwrap (boolean): A boolean indicating that the return value will be
    #   unwrapped.
    # - :async (Hash): A mapping of batch queue system parameters to arguments.
    #   These are passed to the batch queue system only and changes to these must
    #   not affect the return value of the method.
    #
    #  These Procs may accept none, some, or all the arguments that are
    #  defined by margs. Each will be called with the  appropriate number.
    #  For example, if the :pre Proc has arity 2, it will be called with the
    #  first 2 elements of margs.
    #
    # Returns:
    # - Return value of the :result Proc, or nil
    def async_task(margs, command, work_dir, log, args = {})
      unwrap = args.delete(:unwrap)
      async = args.delete(:async) || {}
      mname = calling_method
      env = {}

      callbacks, other = split_task_args(args)
      task_id = task_identity(mname, margs)

      # async_command = async_command(task_id, command, work_dir, log, async)
      asynchronizer = Percolate.asynchronizer
      async_command = asynchronizer.async_command(task_id, command, work_dir,
                                                  log, async)

      result = asynchronizer.async_task(mname, margs,
                                        async_command, env,
                                        callback_defaults.merge(callbacks))
      maybe_unwrap(result, unwrap)
    end

    # Defines an indexed array asynchronous external (batch queue) task method.
    #
    # Arguments:
    #
    # - margs_arrays (Array of Arrays): The argument arrays for each indexed
    #   method call. Each element is an argument Array of a separate call and
    #   contains the arguments that are necessary and sufficient to identify an
    #   invocation of the task. If the task method is called in  multiple contexts
    #   with these same arguments, they will resolve to a single invocation in the
    #    Memoizer.
    # - commands (Array of Strings): The command strings to be executed. This and
    #   the margs_arrays must be the same length.
    # - work_dir (String): The working directory where the batch job will be run.
    # - log (String): The name of the batch queue log file. This may be an
    #   absolute file name, or one relative to the work_dir.
    #
    # Other arguments (keys and values):
    #
    # - :pre (Proc): A precondition callback that must evaluate true before
    #   the task is submitted to the batch queue system. Optional.
    # - :post (Proc): A postcondition callback that must evaluate true before
    #   a result is returned. Optional.
    # - :result (Proc): A return value callback that must evaluate to the desired
    #   return value.
    # - :unwrap (boolean): A boolean indicating that the return value will be
    #   unwrapped.
    # - :async (Hash): A mapping of batch queue system parameters to arguments.
    #   These are passed to the batch queue system only and changes to these must
    #   not affect the return value of the method.
    #
    #  These Procs may accept none, some, or all the arguments that are
    #  defined by margs. Each will be called with the  appropriate number.
    #  For example, if the :pre Proc has arity 2, it will be called with the
    #  first 2 elements of margs.
    #
    # Returns:
    # - Return value of the :result Proc, or nil
    def async_task_array(margs_arrays, commands, work_dir, log, args = {})
      unwrap = args.delete(:unwrap)
      async = args.delete(:async) || {}
      mname = calling_method
      env = {}

      callbacks, other = split_task_args(args)
      task_id = task_identity(mname, margs_arrays)
      array_file = File.join(work_dir, "#{task_id}.txt")

      asynchronizer = Percolate.asynchronizer
      async_command = asynchronizer.async_command(task_id, commands, work_dir, log, async)

      result = asynchronizer.async_task_array(mname, margs_arrays,
                                              commands, array_file,
                                              async_command, env,
                                              callback_defaults.merge(callbacks))
      maybe_unwrap(result, unwrap)
    end

    # A task that should always succeed. It executes the Unix 'true' command.
    #
    # Arguments:
    #
    # - work_dir (String): The working directory. Optional, defaults to '.'
    #
    # Returns:
    #
    # - true.
    def true_task(work_dir = '.')
      task([work_dir], cd(work_dir, 'true'),
           :pre => lambda { work_dir },
           :result => lambda { true })
    end

    # A task that should always fail. It executes the Unix 'false' command.
    #
    # Arguments:
    #
    # - work_dir (String): The working directory. Optional, defaults to '.'
    #
    # Returns:
    #
    # - false.
    def false_task(work_dir = '.')
      task([work_dir], cd(work_dir, 'false'),
           :pre => lambda { work_dir },
           :result => lambda { false })
    end

    private
    # Returns a Symbol naming the calling method.
    def calling_method
      if caller[1] =~ /`([^']*)'/
        $1.to_sym
      else
        raise PercolateError,
              "Failed to determine Percolate method name from '#{caller[0]}'"
      end
    end

    def maybe_unwrap(result, unwrap)
      if result && unwrap != false
        result.value
      else
        result
      end
    end

    def split_task_args(args)
      callbacks = args.reject { |key, value| ![:pre, :post, :result].include?(key) }
      other = args.reject { |key, value| callbacks.keys.include?(key) }

      [callbacks, other]
    end
  end
end
