#!/usr/bin/env ruby

module DBI::DBD::AltPg::Type
  module Binary

    PgTypeMap = {}

    class SimplePgType
      def self.pg_type_name(name)
        return unless name
        name = name.to_s
        warn "Redefining PgTypeMap entry #{name}" if PgTypeMap.key?(name)
        PgTypeMap[name] = self
      end
    end

    def self.array_type(klass)
      name = 'Array' + klass.name.split(/::/)[-1]
      subclass = Class.new(DBI::DBD::AltPg::Type::Binary::Array) do
        @scalar_type = klass
      end
      self.const_set(name.to_sym, subclass)
    end

    module Util
      PgDateEpoch = Date.civil(2000, 01, 01)
      PgTimestampEpoch = DateTime.civil(2000, 01, 01)

      MAX_INT16  = 2**15 - 1
      MAX_UINT16 = 2**16

      MAX_INT32  = 2**31 - 1
      MAX_UINT32 = 2**32

      def self.unpack_int16_big(bytes)
        n = bytes.unpack('n').first
        n > MAX_INT16 ? n - MAX_UINT16 : n
      end

      def self.unpack_int64_big(bytes)
        bytes[0..7].reverse.unpack('q').first
      end

      def self.unpack_int32_big(bytes)
        # Ruby pack/unpack has no network byte order *signed* int
        n = bytes.unpack('N').first
        n > MAX_INT32 ? n - MAX_UINT32 : n
      end
    end # -- module Util

    class Array
      # int32 ndim .............. # of dimensions
      # int32 flags ............. ignored for now
      # oid type, ............... basic element type
      # [int32 dim, int32 lbound] x ndim ......
      # elem ...

      # derived classes set @scalar_type ...

      def self.parse(bytes)
        ndim, type = [ bytes[0,4], bytes[8,4] ].map { |i|
                       Util.unpack_int32_big(i)
                     }
        #scalar_type = @type_map[ type.unpack('N')[0][:binary] ]
        dim_extents = (0 .. ndim-1).collect { |i|
                        Util.unpack_int32_big(bytes[12 + i*8, 4])
                        # Ignore "lbound" -- ruby arrays start at zero
                      }
        nelem = dim_extents.inject(1) { |a,b| a *= b }

        offset = 12 + ndim * 8
        elem = 0.upto(nelem - 1).collect{ |i|
                 sz = Util.unpack_int32_big(bytes[offset,4])
                 obj = @scalar_type.parse(bytes[offset += 4, sz])
                 offset += sz
                 obj
               }

        dim_extents.delete_at(0)
        dim_extents.reverse_each do |len|
          tmp = []
          while elem.length > 0
            tmp << elem.slice!(0, len)
          end
          elem = tmp
        end
        elem
      end
    end # -- class Array

    class Boolean # bool
      def self.parse(bytes)
        bytes == "\001"
      end
    end

    class Date
      # int32 days offset from pg epoch
      def self.parse(bytes)
        Util::PgDateEpoch + Util.unpack_int32_big(bytes)
      end
    end

    class Float8 # double precision
      def self.parse(bytes)
        bytes.unpack('G')[0]
      end
    end

    class Float4 # real
      # XXX assert [1.0].pack('g').size == 4
      def self.parse(bytes)
        bytes.unpack('g')[0]
      end
    end

    class Int8 # bigint
      def self.parse(bytes)
        return Util.unpack_int64_big(bytes)
      end
    end

    class Int4 # integer, int
      def self.parse(bytes)
        return Util.unpack_int32_big(bytes)
      end
    end
    array_type Int4

    class Int2 # smallint
      def self.parse(bytes)
        return Util.unpack_int16_big(bytes)
      end
    end

    class Numeric # decimal

      # include/pgsql/server/utils/numeric.h
      NUMERIC_POS = 0x0000
      NUMERIC_NEG = 0x4000
      NUMERIC_NAN = 0xC000

      def self.parse(bytes)
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

      def self.not_a_number
        @nan ||= begin
                   require 'bigdecimal'
                   BigDecimal.new('NaN')
                 end
      end
    end

    class Timestamp # timestamp without time zone
      # int64 microseconds offset from pg epoch
      # XXX assumes integer_datetimes XXX
      # XXX we ignore timezone for now... XXX
      def self.parse(bytes)
        Util::PgTimestampEpoch + Rational(Util.unpack_int64_big(bytes), 86_400 * 1_000_000)
      end
    end

    class Varchar # character varying
      def self.parse(bytes)
        bytes
      end
    end

  end # -- module Binary
end
