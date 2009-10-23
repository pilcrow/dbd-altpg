# -*- ruby -*-

require 'rake/testtask'

desc "Run unit tests"
Rake::TestTask.new do |t|
  t.test_files = FileList[ "test/tc_*.rb" ]
  t.verbose = false
end
