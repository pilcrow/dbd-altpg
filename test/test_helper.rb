#!/usr/bin/env ruby

$:.unshift ( File.basename(Dir.pwd) == "test" ? '../lib' : './lib' )

require 'dbi'
require 'test/unit'

# Workaround #inspect() bug in ruby-1.9 DelegateClass() subclasses

class DBI::Row; def inspect; super end end
class DBI::ColumnInfo; def inspect; super end end

module TestHelper
  ConnArgs = [ 'dbi:AltPg:postgres', ENV['DBI_USER'] || ENV['USER'], ENV['DBI_PASS']]
end
