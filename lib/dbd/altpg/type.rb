#!/usr/bin/env ruby

module DBI::DBD::AltPg::Type
  module Binary

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

    class Boolean
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

    class Float8
      def self.parse(bytes)
        bytes.unpack('G')
      end
    end

    class Float4
      def self.parse(bytes)
        bytes.unpack('g')
      end
    end

    class Int8
      def self.parse(bytes)
        return Util.unpack_int64_big(bytes)
      end
    end

    class Int4
      def self.parse(bytes)
        return Util.unpack_int32_big(bytes)
      end
    end

    class Int2
      def self.parse(bytes)
        return Util.unpack_int16_big(bytes)
      end
    end

    class Numeric

      # include/pgsql/server/utils/numeric.h
      NUMERIC_POS = 0x0000
      NUMERIC_NEG = 0x4000
      NUMERIC_NAN = 0xC000

      def self.parse(bytes)
        # u16 ndigits, i16 weight, u16 sign, u16 dscale
        ndigits, sign, dscale = bytes.unpack('n xx n n')
        weight = Util.unpack_int16_big(bytes[2,2])

        return not_a_number if sign == NUMERIC_NAN

        offset = 8
        r = 0

        ndigits.downto(1) do
          r += Util.unpack_int16_big(bytes[offset,2]) * (10_000**weight)
          weight -= 1
          offset += 2
        end
        r = r.to_f if r.is_a?(::Rational)
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

    class Timestamp
      # int64 microseconds offset from pg epoch
      # XXX assumes integer_datetimes XXX
      # XXX we ignore timezone for now... XXX
      def self.parse(bytes)
        Util::PgTimestampEpoch + Rational(Util.unpack_int64_big(bytes), 86_400 * 1_000_000)
      end
    end

    class Varchar
      def self.parse(bytes)
        bytes
      end
    end

  end # -- module Binary
end
