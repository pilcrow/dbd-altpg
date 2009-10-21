#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgNotify < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)

    @dbh.do('LISTEN ping')
  end

  def teardown
    @dbh.disconnect rescue nil
  end

  def test_notifies_timeout
    r = @dbh.func(:pq_notifies, 3)
    assert(! r)
  end

  def test_notifies_timeout_instant
    r = @dbh.func(:pq_notifies)
    assert(! r)
  end

  def test_notifies_patience
    Thread.new do
      @dbh_2 = DBI.connect(*TestHelper::ConnArgs)
      sleep 3 # Yes, this is ersatz "synchronization"
      @dbh_2.do('NOTIFY ping')
    end
    Thread.pass

    r = @dbh.func(:pq_notifies, 10)

    assert_kind_of(::Array, r)
    assert_equal('ping', r[0])
    assert_kind_of(Numeric, r[1])
  end

  def test_notifies
    @dbh_2 = DBI.connect(*TestHelper::ConnArgs)

    @dbh.do('NOTIFY ping')

    r = @dbh.func(:pq_notifies)
    assert_kind_of(::Array, r)
    assert_equal('ping', r[0])
    assert_kind_of(Numeric, r[1])
  end
end
