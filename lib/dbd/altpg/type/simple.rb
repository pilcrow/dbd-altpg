#!/usr/bin/env ruby

# Basic PG data type unpacking.

require 'dbd/altpg/type/util'
require 'dbd/altpg/type/array'

module DBI::DBD::AltPg::Type

  # Base class for all simple types, handling NULL parsing before delegating
  # behavior to the subclass' unpack() method.
  class SimplePgType
    def self.parse(bytes)
      return nil if bytes.nil?
      unpack(bytes)
    end
  end

  # Convenience function to define and register new simple types
  #
  #   simple_pg_type :Foo => :foo
  #
  # will create ..::Type::Foo and ...::Type::ArrayFoo for unpacking data, and
  # also register the new classes in the Type::PgTypeMap under the indicated
  # pg_catalog.pg_type.typname -- "foo" and the implied "_foo" in the example
  # above.
  def self.simple_pg_type(typemap, &p)
    raise ArgumentError, "improper simple_pg_type definition" unless typemap.size == 1

    name, pg_typnames = typemap.to_a[0]
    pg_typnames = [pg_typnames] unless pg_typnames.is_a?(::Array)

    scalar_class = ::Class.new(SimplePgType)
    scalar_class.instance_eval(&p)
    DBI::DBD::AltPg::Type.const_set(name, scalar_class)

    aryname = ('Array' + name.to_s).to_sym
    ary_class = ::Class.new(Array) do @element_type = scalar_class end
    ary_class = DBI::DBD::AltPg::Type.const_set(aryname, ary_class)
    #puts "#{aryname}:@scalar_class = #{scalar_class}"

    for typname in pg_typnames
      typname = typname.to_s
      warn "Redefining PgTypeMap entry #{name}" if PgTypeMap.key?(typname)
      PgTypeMap[typname]       = scalar_class
      PgTypeMap['_' + typname] = ary_class
    end
  end

  simple_pg_type :Boolean => :bool do
    def unpack(bytes)
      bytes == "\001"
    end
  end

  simple_pg_type :Date => :date do
    def unpack(bytes)
      # int32 days offset from pg epoch
      Util::PgDateEpoch + Util.unpack_int32_big(bytes)
    end
  end

  simple_pg_type :DoublePrecision => :float8 do
    def unpack(bytes)
      # XXX assert [1.0].pack('G').size == 4
      bytes.unpack('G')[0]
    end
  end

  simple_pg_type :Real => :float4 do
    def unpack(bytes)
      # XXX assert [1.0].pack('g').size == 4
      bytes.unpack('g')[0]
    end
  end

  simple_pg_type :Bigint => :int8 do
    def unpack(bytes)
      return Util.unpack_int64_big(bytes)
    end
  end

  simple_pg_type :Int => :int4 do
    def unpack(bytes)
      return Util.unpack_int32_big(bytes)
    end
  end

  simple_pg_type :Smallint => :int2 do
    def unpack(bytes)
      return Util.unpack_int16_big(bytes)
    end
  end

  simple_pg_type :Numeric => :numeric do

    # include/pgsql/server/utils/numeric.h
    NUMERIC_POS = 0x0000
    NUMERIC_NEG = 0x4000
    NUMERIC_NAN = 0xC000

    def unpack(bytes)
      # u16 ndigits, i16 weight, u16 sign, u16 dscale (ignored)
      ndigits, sign = bytes.unpack('n xx n')
      weight = Util.unpack_int16_big(bytes[2,2])

      return not_a_number if sign == NUMERIC_NAN

      offset = 8
      r = 0.0

      ndigits.downto(1) do
        r += Util.unpack_int16_big(bytes[offset,2]) * (10_000**weight)
        weight -= 1
        offset += 2
      end
      r = -r if sign == NUMERIC_NEG
      r
    end

    def not_a_number
      @nan ||= begin
                 require 'bigdecimal'
                 BigDecimal.new('NaN')
               end
    end
  end # -- simple_pg_type Numeric

  simple_pg_type :Timestamp => :timestamp do
    # timestamp without time zone
    # int64 microseconds offset from pg epoch
    # XXX assumes integer_datetimes XXX
    # XXX we ignore timezone for now... XXX
    def unpack(bytes)
      Util::PgTimestampEpoch + Rational(Util.unpack_int64_big(bytes), 86_400 * 1_000_000)
    end
  end

  simple_pg_type :CharacterVarying => [ :varchar, :enum ] do
    # Here we override ::parse(), for time efficiency, as this method
    # is quite frequently invoked.  (CharacterVarying is the default handler)
    def parse(bytes)
      bytes
    end
  end
end
