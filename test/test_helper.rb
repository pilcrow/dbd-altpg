#!/usr/bin/env ruby

require 'dbi'
require 'test/unit'

module TestHelper
  ConnArgs = [ 'dbi:AltPg:postgres', ENV['DBI_USER'] || ENV['USER'], ENV['DBI_PASS']]
end
