#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltTypeBytea < Test::Unit::TestCase
  include TestHelper::Assertions

  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
    @dbh.do('CREATE TEMP TABLE test_bytea (b BYTEA)')
  end

  def teardown
  ensure
    @dbh.disconnect rescue nil
  end

  def test_bytea_null
    assert_converted_type(nil, "SELECT NULL::bytea")
  end

  def test_bytea
    @dbh.do(%q|INSERT INTO test_bytea VALUES (E'foo\\\\000bar')|)
    assert_converted_type("foo\x00bar", "SELECT * FROM test_bytea")
  end

  def test_bytea
    assert_converted_type("foo\x00bar", "SELECT ?::bytea", "foo\x00bar")
  end
end
