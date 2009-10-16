#!/usr/bin/env ruby

class DBI::DBD::AltPg::Statement < DBI::BaseStatement
  def []=(key, value)
    case key
    when 'altpg_result_format'
      self.result_format = value
    end
    (@attr ||= {})[key] = value
  end

  def [](key)
    case key
    when 'altpg_result_format'
      self.result_format
    else
      (@attr ||= {})[key]
    end
  end
end #-- class DBI::DBD::AltPg::Database
