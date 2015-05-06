# encoding: utf-8

# AR adapter for using a fibered postgresql connection with EM
# This adapter should be used within Thin or Unicorn with the rack-fiber_pool middleware.
# Just update your database.yml's adapter to be 'em_postgresql'
# to real connection pool size.

require 'em-synchrony/pg'
require 'em-synchrony/activerecord'
require 'active_record/connection_adapters/postgresql_adapter'

module ActiveRecord
  class Base
    def self.em_postgresql_connection(config)
      client = EM::Synchrony::ActiveRecord::ConnectionPool.new(size: config[:pool]) do
        conn = ActiveRecord::ConnectionAdapters::EMPostgreSQLAdapter::Client.new(config.symbolize_keys)
        
        conn.query_options.merge!(:as => :array)
        conn.set_client_encoding(config[:encoding]) if config[:encoding]
        conn.exec("SET time zone 'UTC'")
        conn.exec("SET client_min_messages TO '#{config[:min_messages]}'") if config[:min_messages]
        conn.exec("SET schema_search_path TO '#{config[:schema_search_path]}'") if config[:schema_search_path]
        conn.exec('SET standard_conforming_strings = on') rescue nil
        conn
      end 
      options = [config[:host], config[:username], config[:password], config[:database], config[:port], config[:socket], 0]
      ActiveRecord::ConnectionAdapters::EMPostgreSQLAdapter.new(client, logger, options, config)
    end
  end

  module ConnectionAdapters
    class EMPostgreSQLAdapter < ::ActiveRecord::ConnectionAdapters::PostgreSQLAdapter
      class Client < ::PG::EM::Client
        include EM::Synchrony::ActiveRecord::Client
      end

      include EM::Synchrony::ActiveRecord::Adapter
    end
  end
end
