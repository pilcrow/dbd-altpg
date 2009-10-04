#!/usr/bin/env ruby

require 'dbi'
require 'test/unit'

class TestAltPgReExecute < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect('dbi:AltPg:postgres', ENV['DBI_USER'] || ENV['USER'], ENV['DBI_PASS'])
    @dbh.prepare("CREATE TEMP VIEW v AS SELECT 1 AS one, 2 AS two, 3 AS three UNION ALL SELECT -1, -2, NULL").execute()
  end
  def teardown
    @dbh.disconnect rescue nil
  end

  def test_reexecute
    @dbh.prepare("SELECT * FROM v") do |sth|
      sth.execute
      assert_equal( [ [ 1,  2,  3 ],
                      [-1, -2, nil] ], sth.fetch_all )
      sth.execute
      assert_equal( [ [ 1,  2,  3 ],
                      [-1, -2, nil] ], sth.fetch_all )
    end
  end

end
