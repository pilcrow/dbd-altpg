#!/usr/bin/env ruby

class DBI::DBD::AltPg::Database < DBI::BaseDatabase
 #def initialize(pg_conn, dbd_driver)
 #  super(pg_conn, {}) # FIXME - attributes
  def initialize(conninfo, dbd_driver)
    @parent = dbd_driver
    @type_map = {}

    self.connectdb(conninfo)

    @type_map = generate_type_map
    # initialize type map (adapted from dbd-pg-0.4.3).
    # Initial query results are "text" encoded, e.g.:
    #  "16", "bool"
    #  "17", "bytea"
    #
    # FIXME: handle array (_typname) types
    #
    #map = {}
    #st = DBI::DBD::AltPg::Statement.new(self, <<'eosql', 0)
#SELECT oid, typname FROM pg_type
#WHERE typtype IN ('b', 'e') and typname NOT LIKE E'\\_%'
#eosql
    #st.execute
    #while r = st.fetch
    #  map[r[0].to_i] = r[1]
    #end
    #st.finish
    #@type_map = map
  end

  def disconnect
    # XXX rollback unless in_transaction?
    @handle.finish
  end

  def database_name
    @handle.db
  end

  def ping
    self.do('')
    true # PGRES_COMMAND_EMPTY is not an error for us
  rescue DBI::DatabaseError
    false
  end

  def tables; end
  def columns(table); end

  def prepare(query)
    ps = DBI::SQL::PreparedStatement.new(nil, query)
    nparams = ps.unbound.size
    if nparams > 0
      query = ps.bind( (1..nparams).map {|i| "$#{i}"} )
    end
    DBI::DBD::AltPg::Statement.new(self, query, nparams)
  end

  private

  def query(sql)
    st = DBI::DBD::AltPg::Statement.new(self, sql, 0)
    st.execute
    if block_given?
      while r = st.fetch
        yield(r)
      end
    else
      st.fetch_all
    end
  ensure
    st.finish rescue nil
  end

  def pg_type_to_dbi(typname)
    # Adapted from ruby-dbd-pg-0.3.8
    case typname
    when 'bool'                      then DBI::Type::Boolean
    when 'int8', 'int4', 'int2'      then DBI::Type::Integer
    when 'varchar'                   then DBI::Type::Varchar
    when 'float4','float8'           then DBI::Type::Float
    when 'time', 'timetz'            then DBI::Type::Timestamp
    when 'timestamp', 'timestamptz'  then DBI::Type::Timestamp
    when 'date'                      then DBI::Type::Timestamp
    when 'decimal', 'numeric'        then DBI::Type::Decimal
    #when 'bytea'                     then DBI::DBD::Pg::Type::ByteA
    when 'enum'                      then DBI::Type::Varchar
    else                                  DBI::Type::Varchar
    end

  end

  def generate_type_map
    # {
    #   pg_oid => { :type_name => typname, :dbi_type => DBI::Type::Klass }
    #   ...
    # }
    map = Hash.new(DBI::Type::Varchar)
    sql = <<'eosql'
SELECT oid, typname FROM pg_type
WHERE typtype IN ('b', 'e') and typname NOT LIKE E'\\_%'
eosql
    query(sql) do |oid, typname|
      map[oid.to_i] = { :type_name => typname,
                        :dbi_type  => pg_type_to_dbi(typname) }
    end
    map
  end
end #-- class DBI::DBD::AltPg::Database
