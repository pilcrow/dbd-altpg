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
  end # -- module AltPg

end # -- module DBD
end # -- module DBI

require 'dbd/altpg/driver'
require 'dbd/altpg/database'
require 'dbd/altpg/pq'
