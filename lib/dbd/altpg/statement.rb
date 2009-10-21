#!/usr/bin/env ruby

class DBI::DBD::AltPg::Statement < DBI::BaseStatement
  def []=(key, value)
    case key
    when /^altpg_/
      raise DBI::NotSupportedError, "Option sth['#{key}'] is not supported"
    end
    (@attr ||= {})[key] = value
  end

  def [](key)
    case key
    when /^altpg_/
      raise DBI::NotSupportedError, "Option sth['#{key}'] is not supported"
    else
      (@attr ||= {})[key]
    end
  end
end #-- class DBI::DBD::AltPg::Statement
