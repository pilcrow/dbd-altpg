#!/usr/bin/env ruby

class DBI::DBD::AltPg::Statement < DBI::BaseStatement

  def [](key)
    case key
    when "altpg_statement_name", "altpg_plan"
      @plan.freeze
    when /^altpg_/
      raise DBI::NotSupportedError, "Attribute sth['#{key}'] is not supported"
    else
      (@attr ||= {})[key]
    end
  end

  def []=(key, value)
    case key
    when /^altpg_/
      self[key] # may raise DBI::NotSupported
      raise DBI::ProgrammingError, "Attempt to modify read-only attribute sth['#{key}']"
    end
    (@attr ||= {})[key] = value
  end

end #-- class DBI::DBD::AltPg::Statement
