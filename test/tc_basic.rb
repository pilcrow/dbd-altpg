#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgBasic < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
    @dbh.do('CREATE TEMP TABLE t (i INT, v VARCHAR(64))')
    @dbh.do("CREATE TEMP VIEW v AS SELECT 1 AS one, 2 AS two, 3 AS three UNION ALL SELECT -1, -2, NULL")
  end

  def teardown
    @dbh.disconnect rescue nil
  end

  def test_do
    r = @dbh.do(%q|INSERT INTO t (i, v) SELECT 1, 'foo' UNION SELECT 2, 'bar' UNION SELECT 3, 'baz'|)
    assert_equal(3, r)

    r = @dbh.do('INSERT INTO t SELECT 4, true UNION SELECT 5, false')
    assert_equal(2, r)

    r = @dbh.do(%q|INSERT INTO t SELECT ?, ? WHERE 2.65 = ?|, 6, true, 2.65)
    assert_equal(1, r)

    r = @dbh.do(%q|INSERT INTO t SELECT ?, ? WHERE NOT ?|, 7, nil, false)
    assert_equal(1, r)

    assert_nothing_raised do
      r = @dbh.do('SELECT * FROM t')
    end

    # -- Let's check a type conversion while we're here
    assert_equal( [ [1,   'foo'],
                    [2,   'bar'],
                    [3,   'baz'],
                    [4,  'true'],
                    [5, 'false'],
                    [6,  'true'],
                    [7,    nil ] ],
                 @dbh.select_all('SELECT * FROM t ORDER BY i') )

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
