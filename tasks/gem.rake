# -*- ruby -*-

require 'rake/gempackagetask'

desc 'Build and package as a gem'
spec = Gem::Specification.new do |s|
  s.name = 'dbd-altpg'
  s.platform = Gem::Platform::RUBY
  s.require_path = 'lib'
  s.version = '0.0.1'
  s.summary = 'An alternative DBI-DBD driver for PostgreSQL'
  s.author = 'Mike Pomraning'
  s.email = 'mjp-sw@pilcrow.madison.wi.us'
  s.homepage = 'http://github.com/pilcrow/dbd-altpg'
  s.requirements = 'PostgreSQL libpq library and headers, ruby-dbi'
  s.has_rdoc = true
  s.files = FileList['lib/dbd/*.rb',
                     'lib/dbd/altpg/*.{rb,c,h}',
                     'test/*',
                     'Rakefile',
                     'README',
                     'TODO']
  s.extra_rdoc_files = 'lib/dbd/altpg/pq.c'
  s.extensions = ['lib/dbd/altpg/extconf.rb']
  s.required_ruby_version = '>= 1.8.6'
end

Rake::GemPackageTask.new(spec) do |pkg|
  pkg.gem_spec = spec
  pkg.need_tar = true
end

