#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

module OutputTypeTestCases

  def setup
    super
    @dbh = DBI.connect(*TestHelper::ConnArgs)
  end

  def teardown
    super
  ensure
    @dbh.disconnect rescue nil
  end

  def assert_converted_type(expected, sql, *params)
    row = @dbh.select_one(sql, *params)
    unless row.is_a? ::DBI::Row and row.size == 1
      raise ArgumentError, "Improper SQL query fed to select_one_column"
    end
    value = row[0]

    # XXX - this kind_of? test is highly dubious, and un-Rubyish in its
    #       demands.  Who cares if ::bigint is a BigDecimal or Bignum or
    #       Math::My::Extension, so long as it looks, sounds and quacks
    #       like a duck.
    assert_kind_of(expected.class, value)

    message = build_message message, '<?> is not <?> (?)', value, expected, sql
    assert_equal(expected, value, message)
  end

  def test_boolean # a.k.a. bool
    assert_converted_type(false, "SELECT false")
    assert_converted_type(false, "SELECT 'foo' IS NULL")
    assert_converted_type(false, "SELECT 'f'::BOOLEAN")
    assert_converted_type(false, "SELECT NOT true")
    assert_converted_type(false, "SELECT 1 = 0")

    assert_converted_type(true, "SELECT true")
    assert_converted_type(true, "SELECT NULL IS NULL")
    assert_converted_type(true, "SELECT 't'::BOOLEAN")
    assert_converted_type(true, "SELECT NOT false")
    assert_converted_type(true, "SELECT 1337 = 1337")
  end

  def test_smallint # a.k.a. int2
    max_int16 = 2**15 - 1
    assert_converted_type(0, "SELECT 0::smallint")
    assert_converted_type(-1, "SELECT -1::smallint")
    assert_converted_type(1, "SELECT 1::smallint")
    assert_converted_type(max_int16, "SELECT  (2^15 - 1)::smallint")
    assert_converted_type(-max_int16, "SELECT -(2^15 - 1)::smallint")
  end

  def test_integer # a.k.a. int, int4
    max_int32 = 2**31 - 1
    assert_converted_type(0, "SELECT 0::integer")
    assert_converted_type(-1, "SELECT -1::integer")
    assert_converted_type(1, "SELECT 1::integer")
    assert_converted_type(max_int32, "SELECT (2^31 - 1)::integer")
    assert_converted_type(-max_int32, "SELECT -(2^31 - 1)::integer")
  end

  def test_bigint # a.k.a. int8
    # Well, 'int8' support isn't actually work on my platform,
    # meaning that PG bigint/int8 support can't actually reach
    # 2^63-1 ...
    # :(
    assert_converted_type(0, "SELECT 0::bigint")
    assert_converted_type(-1, "SELECT -1::bigint")
    assert_converted_type(1, "SELECT 1::bigint")
  end

  def test_real # a.k.a. float4
    assert_converted_type(0.0, "SELECT 0::real")
    assert_converted_type(0.001, "SELECT 0.001::real")
    assert_converted_type(-0.001, "SELECT -0.001::real")
  end

  def test_double_precision # a.k.a. float8
    assert_converted_type(0.0, "SELECT 0::double precision")
    assert_converted_type(0.001, "SELECT 0.001::double precision")
    assert_converted_type(-0.001, "SELECT -0.001::double precision")
  end

  def test_character_varying # a.k.a. varchar
    assert_converted_type('The "Principle of Least Surprise" means least surprise to *me*',
                          %q(SELECT 'The "Principle of Least Surprise" means least surprise to *me*'::character varying))
    assert_converted_type('', "SELECT ''::character varying")
  end

  def test_text
    assert_converted_type('Lorem ipsum dolor sit amet, consectetur adipisicing elit,', "SELECT 'Lorem ipsum dolor sit amet, consectetur adipisicing elit,'::text")
    assert_converted_type('', "SELECT ''::text")
  end

  def test_enum
    # Yes, this is setup() work.  Please do not tell any Test::Unit::Zealots.
    @dbh.do("DROP TYPE IF EXISTS dbi_test_enum")
    @dbh.do("CREATE TYPE dbi_test_enum AS ENUM ('foo', 'bar', 'baz')")
    assert_converted_type('foo', "SELECT 'foo'::dbi_test_enum")
    assert_converted_type('bar', "SELECT 'bar'::dbi_test_enum")
    assert_converted_type('baz', "SELECT 'baz'::dbi_test_enum")
  ensure
    @dbh.do("DROP TYPE IF EXISTS dbi_test_enum")
  end

  def test_date
    assert_converted_type(Date.parse('1825-10-09'),
                          "SELECT '1825-10-09'::date")
  end

  def test_timestamp
    assert_converted_type(DateTime.parse('2004-02-29 23:59:39'),
                          "SELECT '2004-02-29 23:59:39'::timestamp without time zone")
  end

  def test_timestamp_with_time_zone # a.k.a. timestamptz
    pg = @dbh.select_one("SELECT '2009-10-15 23:59:59.9-05'::timestamp with time zone")[0]
    puts "#{DateTime.parse('2009-10-15T23:59:59.9-05:00').to_s} ... #{pg.to_s}"
    assert_converted_type(DateTime.parse('2009-10-15T23:59:59.9-05:00').to_s,
                          "SELECT '2009-10-15 23:59:59.9-05'::timestamp with time zone")

  end
end

class TestAltPgProtocolText < Test::Unit::TestCase
  include OutputTypeTestCases

  def setup
    super
    @dbh['altpg_row_format'] = 0
  end
end

class TestAltPgProtocolBinary < Test::Unit::TestCase
  include OutputTypeTestCases
  def setup
    super
    @dbh['altpg_row_format'] = 1
  end
end

