#!/usr/bin/env ruby

module DBI
module DBD

  module AltPg
    VERSION = '0.0.1'
    DESCRIPTION = 'PostgreSQL DBI DBD'

    # see DBI::TypeUtil#convert
    def self.driver_name
      "AltPg"
    end

    # :nodoc:
    Pg_Regex_ParseParams = %r{         # We look for:
          ["'?]                        # 1. ? params, "identifiers" or
                                       #    'literals' (incl. E'', B''
                                       #    and X'').
      |    --                          # 2. -- SQL comments to end-of-line
      |    /\*                         # 3. /* C-style comments */
      |   \$                           # 4. $Dollar-Quoted-Delimiter$
           ([[:alpha:]_][[:alnum:]]*)?
          \$
    }x     # :nodoc:

    # :nodoc:
    def self.translate_parameters(sql)
      sql = sql.dup
      param = 1
      i = 0

      while i = sql.index(Pg_Regex_ParseParams, i)
        case $~[0]
        when "'", '"'
          # "identifier" or 'literal', advance past closing quote
          i = sql.index($~[0], i + 1) + 1 rescue sql.length
        when "--"
          # SQL-style comment, advance past end-of-line
          i = sql.index(?\n, i+2) + 1 rescue sql.length
        when "/*"
          # C-style comment, advance past close of comment
          i = sql.index("*/", i+2) + 2 rescue sql.length
        when "?"
          # Parameter, substitute $1-style placeholder
          sql[i] = placeholder = "$#{param}"
          i += placeholder.length
          param += 1
        else
          # $$ delimiter, advance past closing delimiter
          # FIXME - potentially unsafe to refer to $~ again, as a when
          #         branch w/ regex might invalidate
          i = sql.index($~[0], i + $~[0].length) + $~[0].length rescue sql.length
        end
      end

      sql
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
require 'dbd/altpg/type'
require 'dbd/altpg/database'
require 'dbd/altpg/statement'
require 'dbd/altpg/pq'
