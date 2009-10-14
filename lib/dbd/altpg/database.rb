#!/usr/bin/env ruby

class DBI::DBD::AltPg::Database < DBI::BaseDatabase
 #def initialize(pg_conn, dbd_driver)
 #  super(pg_conn, {}) # FIXME - attributes
  def initialize(conninfo, dbd_driver)
    @parent = dbd_driver
    @attr = {}

    pq_connect_db(conninfo)

    @type_map = generate_type_map
  end

  def [](key)
    case key
    when 'altpg_client_encoding'
      __show_variable('client_encoding')
    else
      @attr[key]
    end
  end

  def []=(key, value)
    case key
    when 'AutoCommit'
      if value
        self.do('COMMIT') if in_transaction?    # N.B.: *not* self.commit()
      else
        self.do('BEGIN') unless in_transaction?
      end
    when 'altpg_client_encoding'
      __set_variable('client_encoding', value)
    end
    @attr[key] = value
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

  def tables
    # SQL taken from dbd-pg-0.3.9
    make_dbh.select_all(<<'eosql').collect { |row| row[0] }
SELECT
  c.relname
FROM
  pg_catalog.pg_class c
WHERE
  c.relkind IN ('r','v')
    AND
  pg_catalog.pg_table_is_visible(c.oid)
eosql
    end

  def columns(table)
    # love this SQL
    make_dbh.prepare(<<'eosql') do |sth|
SELECT
  a.attname                    AS name,
  t.typname                    AS type_name,
  CASE
    WHEN a.attlen > 0 THEN a.attlen
    WHEN a.atttypmod > 65535 THEN a.atttypmod >> 16
    WHEN a.atttypmod >= 4 THEN a.atttypmod - 4
    ELSE NULL
  END                          AS precision,
  CASE
    WHEN a.attlen <= 0 AND a.atttypmod > 65535 THEN (a.atttypmod & 65535) - 4
    ELSE NULL
  END                          AS scale,
  NOT a.attnotnull             AS nullable,
  d.adsrc                      AS default,
  COALESCE(ii.indexed, false)  AS indexed,
  COALESCE(iu.unique, false)   AS unique,
  COALESCE(ip.primary, false)  AS primary,
  a.atttypid                   AS pg_type,
  a.attlen                     AS pg_typlen
FROM
  pg_catalog.pg_class c
  INNER JOIN
  pg_catalog.pg_attribute a ON c.oid = a.attrelid
  INNER JOIN
  pg_catalog.pg_type t ON t.oid = a.atttypid
  LEFT JOIN
  pg_catalog.pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
  LEFT JOIN
  (SELECT                          -- ii: is column indexed at all?
     ipa.attname AS attname,
     i0.indrelid AS tbl_oid,
     true        AS indexed
   FROM
     pg_catalog.pg_attribute ipa
   INNER JOIN
     pg_catalog.pg_index i0 ON i0.indexrelid = ipa.attrelid
   GROUP BY 1, 2) ii
     ON ii.attname = a.attname AND ii.tbl_oid = c.oid
  LEFT JOIN
  (SELECT                          -- iu: part of a UNIQUE index?
     iua.attname AS attname,
     i1.indrelid AS tbl_oid,
     true        AS unique
   FROM
     pg_catalog.pg_attribute iua
   INNER JOIN
     pg_catalog.pg_index i1 ON i1.indexrelid = iua.attrelid
   WHERE
     i1.indisunique
   GROUP BY 1, 2, 3) iu
     ON iu.attname = a.attname AND iu.tbl_oid = c.oid
  LEFT JOIN
  (SELECT                          -- ip: part of a PRIMARY key?
     ipa.attname AS attname,
     i2.indrelid AS tbl_oid,
     true        AS primary
   FROM
     pg_catalog.pg_attribute ipa
   INNER JOIN
     pg_catalog.pg_index i2 ON i2.indexrelid = ipa.attrelid
   WHERE
     i2.indisprimary
   GROUP BY 1, 2, 3) ip
     ON ip.attname = a.attname AND ip.tbl_oid = c.oid
WHERE
  a.attnum > 0                          -- regular column
    AND
  NOT a.attisdropped                    -- which has not been dropped
    AND
  c.relkind IN ('r','v')                -- belonging to a TABLE or VIEW
    AND
  c.relname = ?                         -- named ?, which TABLE/VIEW is
    AND
  pg_catalog.pg_table_is_visible(c.oid) -- visible without qualification
ORDER BY
  a.attnum ASC
eosql
      sth.execute(table)

      ret = []
      sth.collect do |row|
        h = Hash[ *sth.column_names.zip(row).flatten ]
        h['dbi_type'] = @type_map[ h['type_name'] ]
        h
      end
    end # -- prepare do |sth|
  end

  def prepare(query)
    DBI::DBD::AltPg::Statement.new(self, query)
  end

  def __set_variable(var, value, is_local = false)
    make_dbh.do('SELECT pg_catalog.set_config(?, ?, ?)', var, value, !!is_local)
  rescue ::DBI::DatabaseError => e
    if e.state =~ /^42/ or e.state == "22023" # invalid_parameter_value
      e = ::DBI::ProgrammingError.new(e.message, e.err, e.state)
    end
    raise e
  end

  def __show_variable(var)
    make_dbh.select_one('SELECT pg_catalog.current_setting(?)', var)[0]
  rescue ::DBI::DatabaseError => e
    if e.state =~ /^42/
      e = ::DBI::ProgrammingError.new(e.message, e.err, e.state)
    end
    raise e
  end

  private

  def make_dbh
    dbh = ::DBI::DatabaseHandle.new(self)
    dbh.driver_name = ::DBI::DBD::AltPg.driver_name
    dbh
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

  def pg_type_to_binary(typname)
    sym = case typname
          when 'bool'                      then :Boolean
          when 'int2'                      then :Int2
          when 'int4'                      then :Int4
          when 'int8'                      then :Int8
          when 'varchar'                   then :Varchar
            #FIXME#when 'float4','float8'           then DBI::Type::Float
            #FIXME#when 'time', 'timetz'            then DBI::Type::Timestamp
          when 'timestamp', 'timestamptz'  then :Timestamp
          when 'date'                      then :Date
            #FIXME#when 'decimal', 'numeric'        then DBI::Type::Decimal
            #when 'bytea'                     then DBI::DBD::Pg::Type::ByteA
          when 'enum'                      then :Varchar
          else                                  :Varchar
          end
    return DBI::DBD::AltPg::Type::Binary.const_get(sym)
  end

  def generate_type_map
    # {
    #   pg_oid => { :type_name => typname, :dbi_type => DBI::Type::Klass }
    #   ...
    # }
    #
    # We perform this query "raw," not as a DBI::StatementHandle, since
    # we (obviously) haven't loaded the typemappings needed for the higher
    # layer adapter to function.
    #
    map = Hash.new(
            {:type_name         => 'unknown',
             :text_conversion   => DBI::Type::Varchar,
             :binary_conversion => DBI::DBD::AltPg::Type::Binary::Varchar}
          )
    raw_sth = prepare(<<'eosql')
SELECT
  t.oid,
  t.typname
FROM
  pg_catalog.pg_type t
WHERE
  t.typtype IN ('b', 'e')
  AND
  t.typname NOT LIKE E'\\_%'
eosql
    raw_sth.execute
    while r = raw_sth.fetch
      oid, typname = r
      # XXX - when we go binary, we'll need to unpack
      map[oid.to_i] = { :type_name => typname,
                        :text_conversion   => pg_type_to_dbi(typname),
                        :binary_conversion => pg_type_to_binary(typname) }
    end
    map
  ensure
    raw_sth.finish rescue nil
  end
end #-- class DBI::DBD::AltPg::Database
