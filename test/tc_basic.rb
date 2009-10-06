#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgBasic < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
    @dbh.do('CREATE TEMP TABLE t (i INT)')
    @dbh.do("CREATE TEMP VIEW v AS SELECT 1 AS one, 2 AS two, 3 AS three UNION ALL SELECT -1, -2, NULL")
  end

  def teardown
    @dbh.disconnect rescue nil
  end

  def test_do
    r = @dbh.do('INSERT INTO t SELECT 1 UNION SELECT 2 UNION SELECT 3')
    assert_equal(3, r)

    r = @dbh.do('INSERT INTO t SELECT 4 UNION SELECT 5')
    assert_equal(2, r)

    assert_nothing_raised do
      r = @dbh.do('SELECT * FROM t')
    end
  end

  def test_parameterized_query
    assert_nothing_raised do
      @dbh.prepare('SELECT COUNT(*) FROM v WHERE three IN (?, ?)') do |sth|
        sth.execute(10, 20)
        assert_equal( [ [0] ], sth.fetch_all )
        
        sth.execute(2**18, 3)
        assert_equal( [ [1] ], sth.fetch_all )
      end
    end
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
