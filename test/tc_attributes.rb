#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgAttributes < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
  end

  def teardown
    @dbh.disconnect rescue nil
  end

  def test_attr_socket
    assert_kind_of(Integer, @dbh['altpg_socket'])
    assert_nothing_raised do
      # Must suppress for_fd() IO obj. finalization.
      # See http://redmine.ruby-lang.org/issues/show/2250
      $ruby_bug_2250_workaround = ::IO.for_fd(@dbh['altpg_socket'])
    end

    @dbh.disconnect
    assert_raises(DBI::InterfaceError) do
      @dbh['altpg_socket']
    end
  end

  def test_sth_plan
    @dbh.prepare('SELECT 1 WHERE 1=?::NUMERIC') do |sth|
      assert_not_nil(sth['altpg_plan'])

      assert_not_nil(sth['altpg_statement_name'])

      assert(sth['altpg_plan'].object_id == sth['altpg_statement_name'].object_id,
             "'altpg_plan' and 'altpg_statement_name' are synonyms")
      assert(sth['altpg_plan'].frozen?)

      assert_raises DBI::ProgrammingError do
        sth['altpg_plan'] = "This is not allowed"
      end

      assert_raises DBI::ProgrammingError do
        sth['altpg_statement_name'] = "This is not allowed, either"
      end
    end
  end
end
