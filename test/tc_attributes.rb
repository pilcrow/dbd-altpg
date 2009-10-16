#!/usr/bin/env ruby

require File.dirname(__FILE__) + "/test_helper"

class TestAltPgAttributes < Test::Unit::TestCase
  def setup
    @dbh = DBI.connect(*TestHelper::ConnArgs)
  end

  def teardown
    @dbh.disconnect rescue nil
  end

  def test_sth_inherits_result_format
    @dbh['altpg_result_format'] = 0
    @dbh.prepare('SELECT 1') do |sth|
      assert_equal(0, sth['altpg_result_format'])
    end

    @dbh['altpg_result_format'] = 1
    @dbh.prepare('SELECT 1') do |sth|
      assert_equal(1, sth['altpg_result_format'])
    end
  end

  def test_sth_resfmt_does_not_affect_parent
    @dbh['altpg_result_format'] = 0
    @dbh.prepare('SELECT 1') do |sth|
      sth['altpg_result_format'] = 1
      assert_equal(1, sth['altpg_result_format'])
      assert_equal(0, @dbh['altpg_result_format'])
    end

    @dbh['altpg_result_format'] = 1
    @dbh.prepare('SELECT 1') do |sth|
      sth['altpg_result_format'] = 0
      assert_equal(0, sth['altpg_result_format'])
      assert_equal(1, @dbh['altpg_result_format'])
    end
  end

end
