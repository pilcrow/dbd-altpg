#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgThousands < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
    @sql = "CREATE TEMP VIEW v AS SELECT 1"
  end
  def teardown
    @dbh.disconnect rescue nil
  end

  def test_sth_thousands_finish
    2_500.times do
      @dbh.prepare(@sql).finish
    end
  end

  def test_sth_thousands_leak
    2_500.times do
      @dbh.prepare(@sql)
    end
  end

end
