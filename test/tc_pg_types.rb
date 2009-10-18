#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgTypes < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
  end

  def teardown
  ensure
    @dbh.disconnect rescue nil
  end

  def assert_datetime_equal(expected, actual, msg = nil)
    [:zone, :sec_fraction, :sec, :min, :hour, :mday, :mon, :year].each do |m|
      failmsg = "comparing #{m}"
      failmsg += " #{msg}" if msg
      assert_equal(expected.__send__(m), actual.__send__(m), failmsg)
    end
  end

  def assert_converted_type(expected, sql, *params)
    row = @dbh.select_one(sql, *params)
    unless row.is_a? ::DBI::Row and row.size == 1
      raise ArgumentError, "Improper SQL query fed to select_one_column"
    end
    value = row[0]

    message = build_message message, '<?> is not <?> (?)', value, expected, sql

    if expected.is_a?(::DateTime)
      # sigh
      assert_datetime_equal(expected, value, message)
    else
      assert_equal(expected, value, message)
    end
  end

  def test_boolean # a.k.a. bool
    assert_converted_type(nil, "SELECT NULL::boolean")

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
    assert_converted_type(nil, "SELECT NULL::smallint")

    max_int16 = 2**15 - 1
    assert_converted_type(0, "SELECT 0::smallint")
    assert_converted_type(-1, "SELECT -1::smallint")
    assert_converted_type(1, "SELECT 1::smallint")
    assert_converted_type(max_int16, "SELECT  (2^15 - 1)::smallint")
    assert_converted_type(-max_int16, "SELECT -(2^15 - 1)::smallint")
  end

  def test_integer # a.k.a. int, int4
    assert_converted_type(nil, "SELECT NULL::integer")

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
    assert_converted_type(nil, "SELECT NULL::bigint")

    assert_converted_type(0, "SELECT 0::bigint")
    assert_converted_type(-1, "SELECT -1::bigint")
    assert_converted_type(1, "SELECT 1::bigint")
  end

  def test_real # a.k.a. float4
    assert_converted_type(nil, "SELECT NULL::real")

    assert_converted_type(0.0, "SELECT 0::real")
    assert_converted_type(10.0, "SELECT 10::real")
    assert_converted_type(-10.0, "SELECT -10::real")
    assert_converted_type(0.2, "SELECT 0.2::real")
    assert_converted_type(-0.2, "SELECT -0.2::real")
  end

  def test_double_precision # a.k.a. float8
    assert_converted_type(nil, "SELECT NULL::double precision")

    assert_converted_type(0.0, "SELECT 0::double precision")
    assert_converted_type(0.001, "SELECT 0.001::double precision")
    assert_converted_type(-0.001, "SELECT -0.001::double precision")
  end

  def test_character_varying # a.k.a. varchar
    assert_converted_type(nil, "SELECT NULL::character varying")

    assert_converted_type('The "Principle of Least Surprise" means least surprise to *me*',
                          %q(SELECT 'The "Principle of Least Surprise" means least surprise to *me*'::character varying))
    assert_converted_type('', "SELECT ''::character varying")
  end

  def test_text
    assert_converted_type(nil, "SELECT NULL::text")

    assert_converted_type('Lorem ipsum dolor sit amet, consectetur adipisicing elit,', "SELECT 'Lorem ipsum dolor sit amet, consectetur adipisicing elit,'::text")
    assert_converted_type('', "SELECT ''::text")
  end

  def test_enum
    # Yes, this is setup() work.  Please do not tell any Test::Unit::Zealots.
    @dbh.do("DROP TYPE IF EXISTS dbi_test_enum")
    @dbh.do("CREATE TYPE dbi_test_enum AS ENUM ('foo', 'bar', 'baz')")
    assert_converted_type(nil, "SELECT NULL::dbi_test_enum")

    assert_converted_type('foo', "SELECT 'foo'::dbi_test_enum")
    assert_converted_type('bar', "SELECT 'bar'::dbi_test_enum")
    assert_converted_type('baz', "SELECT 'baz'::dbi_test_enum")
  ensure
    @dbh.do("DROP TYPE IF EXISTS dbi_test_enum")
  end

  def test_date
    assert_converted_type(nil, "SELECT NULL::date")

    assert_converted_type(Date.parse('1825-10-09'),
                          "SELECT '1825-10-09'::date")
  end

  def test_timestamp
    assert_converted_type(nil, "SELECT NULL::timestamp")
    # XXX - high precision?
    assert_converted_type(DateTime.parse('2004-02-29 23:59:39'),
                          "SELECT '2004-02-29 23:59:39'::timestamp without time zone")
    assert_converted_type(DateTime.parse('1999-09-09 09:09:09.0909'),
                          "SELECT '1990-09-09 09:09:09.0909'::timestamp without time zone + INTERVAL '9 YEARS'")
  end

  ## FIXME ##
  ##def test_timestamp_with_time_zone # a.k.a. timestamptz
  ##  assert_converted_type(nil, "SELECT NULL::timestamp with time zone")
  ##  assert_converted_type(DateTime.parse('2009-10-15T23:59:59.9-05:00'),
  ##                        "SELECT '2009-10-15 23:59:59.9-05'::timestamp with time zone")
  ##end

  def test_numeric # a.k.a. decimal
    assert_converted_type(nil, "SELECT NULL::NUMERIC")

    assert_converted_type(0, "SELECT 0::NUMERIC")
    assert_converted_type(0.0, "SELECT 0.0::NUMERIC")
    assert_converted_type(0.1, "SELECT 0.1::NUMERIC")
    assert_converted_type(1, "SELECT 1::NUMERIC")
    assert_converted_type(-1, "SELECT -1::NUMERIC")
    assert_converted_type(-0.1, "SELECT -0.1::NUMERIC")
    assert_converted_type(12_345_678, "SELECT 12345678::NUMERIC")
    assert_converted_type(-12_345_678.9, "SELECT -12345678.9::NUMERIC")
    assert_converted_type(-10_001.000056, "SELECT -10001.000056::NUMERIC")
  end

  def test_array
    assert_converted_type(nil, "SELECT NULL::integer[][]")

    assert_converted_type([ 1, 2, 3 ],
                          "SELECT ARRAY[ 1, 2, 3 ]")
    assert_converted_type([ [ ['a', 'b'], ['y', 'z'] ],
                            [ ['0', '1'], ['8', '9'] ],
                            [ ['~', '!'], ['_', '+'] ] ],
                          <<'__eosql')
SELECT ARRAY[
              ARRAY[ ARRAY['a', 'b'], ARRAY['y', 'z'] ],
              ARRAY[ ARRAY['0', '1'], ARRAY['8', '9'] ],
              ARRAY[ ARRAY['~', '!'], ARRAY['_', '+'] ]
            ]
__eosql
  end

end
