
require 'rake'
require 'rake/clean'
require 'rake/gempackagetask'
require 'rake/rdoctask'
require 'rake/testtask'
require 'rcov/rcovtask'

spec = Gem::Specification.new do |spec|
  spec.name = 'percolate'
  spec.version = '0.1.0'
  spec.has_rdoc = true
  spec.extra_rdoc_files = ['README', 'LICENSE']
  spec.summary = 'The Percolate workflow utility.'
  spec.description = spec.summary
  spec.author = 'Keith James'
  spec.email = 'kdj@sanger.ac.uk'
  spec.executables = ['percolate-rb', 'wrapper']
  spec.files = %w(LICENSE README Rakefile) + Dir.glob('{bin,lib,spec}/**/*')
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
  t.test_files = FileList['test/**/*.rb']
end

Rcov::RcovTask.new do |rcov|
  rcov.pattern    = FileList['test/**/*.rb']
  rcov.output_dir = 'coverage'
  rcov.verbose    = true
  rcov.rcov_opts << "--sort coverage -x 'rcov,ruby'"
end
