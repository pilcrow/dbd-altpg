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
    Pg_Regex_LocateAction = %r{
           --                          # -- SQL comment to end-of-line
      |    /\*                         # /* C-style comment */
      |    \S+                         # Non-commented "token"
    }x     # :nodoc:

    # :nodoc:
    Pg_Regex_ParseParams  = %r{        # We look for:
          ["'?]                        # 1. ? params, "identifiers" or
                                       #    'literals' (incl. E'', B''
                                       #    and X'').
      |    --                          # 2. -- SQL comments to end-of-line
      |    /\*                         # 3. /* C-style comments */
      |   \$                           # 4. $Dollar-Quoted-Delimiter$
           ([[:alpha:]_][[:alnum:]]*)?
          \$
    }x     # :nodoc:

    # translate_sql(sql) -> [ subst_sql, action, count_params ]
    #
    # Translate the given sql string, transforming ?-style placeholders into
    # $1-styles.  Returns the transformed sql query, the lowercased "action"
    # of the query (the first non-whitespace part of the query after any
    # comments, normally "SELECT", "INSERT", "DROP", etc.), and the number of
    # parameter substitutions made.
    #
    # This method expects to receive a single SQL query; it does not
    # understand multiple queries in a single string.
    #
    # Note that if zero substitutions have been made, the returned sql string
    # may be the same object as passed into the method.
    #
    # +action+ may be +nil+ if, after ignoring any comments, the sql query
    # is empty.
    def self.translate_sql(sql)
      i = 0
      action = nil
      param_no = 1

      # Extract and normalize the action, if any, skipping any leading
      # comments
      while i = sql.index(Pg_Regex_LocateAction, i)
        case $~[0]
        when '--'
          i = sql.index(?\n, i+2) + 1 rescue sql.length
        when '/*'
          i = sql.index("*/", i+2) + 2 rescue sql.length
        else
          action = $~[0].downcase
          break
        end
      end

      while i = sql.index(Pg_Regex_ParseParams, i) rescue nil
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
          sql = sql.dup if param_no == 1

          # Parameter, substitute $1-style placeholder
          sql[i] = placeholder = "$#{param_no}"
          i += placeholder.length
          param_no += 1
        else
          # $$ delimiter, advance past closing delimiter
          # FIXME - potentially unsafe to refer to $~ again, as a when
          #         branch w/ regex might invalidate
          i = sql.index($~[0], i + $~[0].length) + $~[0].length rescue sql.length
        end
      end

      return [sql, param_no - 1, action]
    end

  end # -- module AltPg

end # -- module DBD
end # -- module DBI

# All the heavy lifting is done in AltPg::Statement.bind_param
DBI::TypeUtil.register_conversion(DBI::DBD::AltPg.driver_name) do |obj|
  [obj, false]
end

require 'dbd/altpg/driver'
require 'dbd/altpg/type'
require 'dbd/altpg/database'
require 'dbd/altpg/statement'
require 'dbd/altpg/pq'
