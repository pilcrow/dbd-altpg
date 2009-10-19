#   simple_pg_type :Foo => :foo
#
# will create ..::Type::Foo and ...::Type::ArrayFoo for unpacking data, and
# also register the new classes in the Type::PgTypeMap under the indicated
# pg_catalog.pg_type.typname -- "foo" and the implied "_foo" in the example
# above.
#
module DBI::DBD::AltPg::Type
  # Base class for simple types (see altpg/type/simple.rb), handling NULLs
  class SimplePgType
    def self.parse(bytes)
      return nil if bytes.nil?
      unpack(bytes)
    end
  end

  # Convenience to register new output type converter classes for both a
  # given, simple type and its implied array type.
  #
  #  simple_pg_type :Foo => :foo
  #
  # will map PostgreSQL type 'foo' (pg_catalog.pg_type.typname = 'foo') and
  # ARRAY[] type '_foo' to Type::Foo and Type::ArrayFoo, respectively.
  #
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

  # Convenience module to define a the pg epoch, and to let us unpack signed
  # integral values in network byte order.
  module Util
    PgDateEpoch      = Date.civil(2000, 01, 01)
    PgTimestampEpoch = DateTime.civil(2000, 01, 01)

    if [1].pack('l') == "\001\000\000\000"  # little-endian
      def self.unpack_int16_big(bytes)
        bytes[0,2].reverse!.unpack('s').first
      end
      def self.unpack_int32_big(bytes)
        bytes[0,4].reverse!.unpack('l').first
      end
      def self.unpack_int64_big(bytes)
        bytes[0,8].reverse!.unpack('q').first
      end
    else # -- unpacking for big endian systems
      def self.unpack_int16_big(bytes)
        bytes.unpack('s').first
      end
      def self.unpack_int32_big(bytes)
        bytes.unpack('l').first
      end
      def self.unpack_int64_big(bytes)
        bytes.unpack('q').first
      end
    end
  end # -- module Util
end
