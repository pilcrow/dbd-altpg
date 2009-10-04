class DBI::DBD::AltPg::Driver < DBI::BaseDriver
  def initialize
    super('0.4.0')
  end

  def connect(dbname, user, auth, attr)
    s = []
    s << "dbname='#{dbname}'" if dbname
    s << "user='#{user}'" if user
    s << "password='#{auth}'" if auth

    # FIXME - attributes
    #pq = DBI::DBD::AltPg::Pq.new(s.join(''))
    #DBI::DBD::AltPg::Database.new( pq, self )
    DBI::DBD::AltPg::Database.new( s.join(''), self )
  end
end
