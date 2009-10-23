#!/usr/bin/env ruby

class DBI::DBD::AltPg::Statement < DBI::BaseStatement
  def []=(key, value)
    case key
    when "altpg_statement_name", "altpg_plan"
      raise DBI::ProgrammingError, "Attempt to modify read-only attribute sth['#{key}']"
    when /^altpg_/
      raise DBI::NotSupportedError, "Option sth['#{key}'] is not supported"
    end
    (@attr ||= {})[key] = value
  end

  def [](key)
    case key
    when "altpg_statement_name", "altpg_plan"
      @plan.freeze
    when /^altpg_/
      raise DBI::NotSupportedError, "Option sth['#{key}'] is not supported"
    else
      (@attr ||= {})[key]
    end
  end
end #-- class DBI::DBD::AltPg::Statement
