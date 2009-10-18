#!/usr/bin/env ruby

# PG binary protocol (resultFormat = 1) output conversions
#
# PgTypeMap maps a pg_catalog.pg_type.typname to a ruby class responsible
# for unpacking the type's wire format via Klass.unpack().

module DBI::DBD::AltPg::Type
  PgTypeMap = Hash.new do |hsh,key|
                (key =~ /^_/) ? hsh.default_array : hsh.default_simple
              end
  class << PgTypeMap
    attr_accessor :default_simple
    attr_accessor :default_array
  end
end

require 'dbd/altpg/type/array'
require 'dbd/altpg/type/simple'

module DBI::DBD::AltPg::Type
  PgTypeMap.default_simple = CharacterVarying
  PgTypeMap.default_array  = ArrayCharacterVarying
end
