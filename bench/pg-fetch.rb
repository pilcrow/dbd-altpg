#!/usr/bin/env ruby

require 'rubygems'
require 'dbi'
require 'profiler'
require 'benchmark'

@sql = 'SELECT * FROM dataview'

DBI.connect(*ARGV) do |dbh|
  dbh.do('DROP VIEW dataview') rescue nil
  dbh.do('CREATE TEMP VIEW dataview AS SELECT * FROM pg_catalog.pg_type UNION ALL SELECT * FROM pg_catalog.pg_type')

  #Profiler__::start_profile
  #2.times { dbh.select_all(@sql) }
  #Profiler__::stop_profile
  #next
  Benchmark.bm(20) do |bm|
    #bm.report('select_all') { 100.times { dbh.select_all(@sql) } }
    dbh.prepare(@sql) do |sth|
      bm.report('execute/fetch_all') { 50.times { sth.execute; sth.fetch_all } }
      bm.report('execute/fetch_one') { 500.times { sth.execute; sth.fetch } }
    end
  end
  dbh.do('DROP VIEW dataview') rescue nil
end

#Profiler__::print_profile($stdout)
