#!/usr/bin/env ruby

module DBI::DBD::AltPg::Type
  module Binary

    module Util
      PgDateEpoch = Date.civil(2000, 01, 01)
      PgTimestampEpoch = DateTime.civil(2000, 01, 01)

      MAX_INT16  = 2**16 - 1
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
