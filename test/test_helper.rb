#!/usr/bin/env ruby

$:.unshift ( File.basename(Dir.pwd) == "test" ? '../lib' : './lib' )

require 'dbi'
require 'test/unit'

# Workaround #inspect() bug in ruby-1.9 DelegateClass() subclasses

class DBI::Row; def inspect; super end end
class DBI::ColumnInfo; def inspect; super end end

module TestHelper
  ConnArgs = [ 'dbi:AltPg:postgres', ENV['DBI_USER'] || ENV['USER'], ENV['DBI_PASS']]

  module Assertions
    def assert_converted_type(expected, sql, *params)
      row = @dbh.select_one(sql, *params)
      unless row.is_a? ::DBI::Row and row.size == 1
        raise ArgumentError, "Improper SQL query fed to select_one_column"
      end
      value = row[0]

      message = build_message message, '<?> is not <?> (?)', value, expected, sql

      if expected.is_a?(::DateTime)
        #
        # Consistent with PostgreSQL's implementation, two DateTime objects
        # are equivalent if they refer to the same absolute point in calendar
        # time:
        #
        #   postgres=> SELECT    '2000-01-01 01:00:00+01'::timestamptz
        #   postgres->              =
        #   postgres->           '2000-01-01 00:00:00-00'::timestamptz;
        #    ?column?
        #   ----------
        #    t
        #   (1 row)
        #
        # So, normalize dt.zone() for the comparison
        #
        expected, value = [expected, value].map { |dt| dt.dup.new_offset(0) }
      end

      assert_equal(expected, value, message)
    end
  end
end
