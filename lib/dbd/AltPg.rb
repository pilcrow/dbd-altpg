#!/usr/bin/env ruby

module DBI
module DBD

  module AltPg
    VERSION = 'pre-20091001'
    DESCRIPTION = 'PostgreSQL DBI DBD'
 
    # see DBI::TypeUtil#convert
    def self.driver_name
      "AltPg"
    end

    def self.translate_parameters(query) # :nodoc:
      ps = DBI::SQL::PreparedStatement.new(nil, query)
      nparams = ps.unbound.size
      if nparams > 0
        query = ps.bind( (1..nparams).map {|i| "$#{i}"} )
      end
      return [query, nparams]
    end

  end # -- module AltPg

end # -- module DBD
end # -- module DBI

DBI::TypeUtil.register_conversion(DBI::DBD::AltPg.driver_name) do |obj|
  # dbi-0.4.3 String default conversion broken for native binding, since
  # the former adds quotes, which should be done by native binding
  case obj
  when ::String
    [obj, false]
  when ::TrueClass, ::FalseClass
    [obj.to_s, false]
  else
    [obj, true]
  end
end

require 'dbd/altpg/driver'
require 'dbd/altpg/database'
require 'dbd/altpg/pq'
