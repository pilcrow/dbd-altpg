# -*- ruby -*-

namespace :ext do
  ext_dir = 'lib/dbd/altpg/'
  ext_sources = FileList["#{ext_dir}/*.{c,h}"]
  ext_libname = File.join(ext_dir, "pq.#{Config::CONFIG['DLEXT']}")
  ext_extconf = File.join(ext_dir, 'extconf.rb')
  ext_makefile = File.join(ext_dir, 'Makefile')

  file ext_makefile => ext_extconf do
    chdir(ext_dir) { ruby "extconf.rb" }
  end

  file ext_libname => ext_sources + [ext_makefile] do
    chdir(ext_dir) { sh "make" }
  end

  task :build => ext_libname

  task :clean do
    chdir(ext_dir) { sh "make clean" if test ?e, 'Makefile' }
  end

end
