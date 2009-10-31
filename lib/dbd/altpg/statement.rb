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

  def bind_param(i, value, extra)
    translated = case value
                 when nil
                   [nil, :unknown, 0]
                 when TrueClass
                   #["\1", :bool, 1 ]
                   ["t", :bool, 0 ]
                 when FalseClass
                   #["\0", :bool, 1 ]
                   ["f", :bool, 0 ]
                 when String
                   [ value, :varchar, 1 ]
                 #when DBI::DBD::AltPg::Type::ByteA
                 #  [ value, :bytea, 1 ]
                 when ::Date
                   [ value.strftime('%Y-%m-%d'), :date, 0 ]
                 when ::Time, ::DateTime
                   [ value.strftime("%Y-%m-%dT%H:%M:%S%Z"), :timestamptz, 0 ]
                 when BigDecimal
                   [ value.to_s('F'), :numeric, 0 ]
                 when Numeric
                   [ value.to_s, :numeric, 0 ]
                 else
                   [ value.to_s, :unknown, 0 ]
                 end
    translated[1] = @type_map[ translated[1] ][:oid]
    @params[i - 1] = translated
  end
end #-- class DBI::DBD::AltPg::Statement
