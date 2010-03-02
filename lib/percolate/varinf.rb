
module Varinf
  include Percolate

  class VarinfWorkflow < Workflow
    def run *args
      source_host = 'localhost'
      source_path = '/home/keith'
      dest_file = 'foo.txt'
      work_dir = '/home/keith/tmp'
      log = 'rsync_foo.log'

      async_sleep 30, work_dir, log
    end
  end
end
