require 'rake'
require 'rake/clean'
require 'rake/gempackagetask'
require 'rake/rdoctask'
require 'rake/testtask'
require 'rcov/rcovtask'

require 'lib/percolate/version'

spec = Gem::Specification.new do |spec|
  spec.name = 'percolate'
  spec.version = Percolate::VERSION
  spec.extra_rdoc_files = ['README', 'LICENSE']
  spec.summary = 'The Percolate workflow utility.'
  spec.description = 'Percolate is a lightweight library for coordinated ' +
      'execution of command-line programs. It provides a means of defining data ' +
      'transformation pipelines as simple scripts.'
  spec.author = 'Keith James'
  spec.email = 'kdj@sanger.ac.uk'
  spec.executables = ['percolate', 'percolate-audit', 'percolate-wrap',
                      'percolate-queues']
  spec.files = %w(LICENSE README Rakefile) + Dir.glob('{bin,spec}/**/*') +
      Dir.glob('lib/**/*.rb')
  spec.require_path = 'lib'
  spec.bindir = 'bin'
end

Rake::GemPackageTask.new(spec) do |pack|
  pack.gem_spec = spec
  pack.need_tar = true
  pack.need_zip = false
end

Rake::RDocTask.new do |rdoc|
  files =['README', 'LICENSE', 'lib/**/*.rb']
  rdoc.rdoc_files.add(files)
  rdoc.main = "README" # page to start on
  rdoc.title = "Percolate Documentation"
  rdoc.rdoc_dir = 'doc/rdoc' # rdoc output folder
  rdoc.options << '--line-numbers'
end

Rake::TestTask.new do |t|
  t.test_files = FileList['test/**/test_*.rb']
end

Rcov::RcovTask.new do |rcov|
  rcov.pattern = FileList['test/**/test_*.rb']
  rcov.output_dir = 'coverage'
  rcov.verbose = true
  rcov.rcov_opts << "--sort coverage -x 'rcov,ruby'"
end
