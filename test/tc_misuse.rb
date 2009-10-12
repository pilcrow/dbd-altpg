#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgMisuse < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
    @dbh.do('CREATE TEMP TABLE t (n NUMERIC, v VARCHAR(64))')
  end

  def teardown
    @dbh.disconnect rescue nil
  end

  def test_do_wrong_param_count
    sql = 'INSERT INTO t VALUES (?, ?)'
    assert_raises(DBI::DatabaseError) do
      @dbh.do( sql, 3.14159265, 'A man a plan a canal, Panama', 'extraneous' )
    end

    assert_nothing_raised do
      r = @dbh.do( sql, 3.14159265, 'A man a plan a canal, Panama' )
      assert_equal( 1, r )

      r = @dbh.do( sql, 13, 'Lorem ipsum dolor sit amet' )
      assert_equal( 1, r )
    end

    assert_raises(DBI::DatabaseError) do
      r = @dbh.do( sql, 3.14159265 )
    end

    assert_nothing_raised do
      r = @dbh.do( sql, -1, 'foo' )
      assert_equal( 1, r )
    end
  end

  def test_exec_wrong_param_count
    sth = @dbh.prepare('SELECT ?::NUMERIC, ?::VARCHAR')
    assert_raises(DBI::DatabaseError) do
      sth.execute( 3.14159265, 'A man a plan a canal, Panama', 'extraneous' )
    end

    assert_nothing_raised do
      sth.execute( 3.14159265, 'A man a plan a canal, Panama' )
      assert_equal( [ [ 3.14159265, 'A man a plan a canal, Panama' ] ],
                    sth.fetch_all )

      sth.execute( 13, 'Lorem ipsum dolor sit amet' )
      assert_equal( [ [ 13, 'Lorem ipsum dolor sit amet' ] ],
                    sth.fetch_all )
    end

    assert_raises(DBI::DatabaseError) do
      sth.execute( 3.14159265 )
    end

    assert_nothing_raised do
      sth.execute( -1, 'foo' )
      assert_equal( [ [ -1, 'foo' ] ], sth.fetch_all )
    end

    sth.finish
  end

end
