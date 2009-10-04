#!/usr/bin/env ruby

require 'mkmf'

['pq'].each do |m|
  dir_config(m)
  have_library('pq')
  create_makefile('pq')
end
