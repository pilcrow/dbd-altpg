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

end
