# rubocop:disable Style/FileName

# By default, cql-rb grabs a random connection for each request. This is great for
# keeping load distributed across the cluster. Unfortunately, it plays havoc with
# the Solr integration where we often need to ensure that we're talking to the
# Solr and Cassandra on the same node. For that reason, we cause the current
# connection in use to stay fixed for 500 requests before rolling to the another.
require 'cql'
require 'cql/client/connection_manager'

Cql::Client::ConnectionManager.class_eval do
  attr_reader :current_connection

  def random_connection
    fail ::Cql::NotConnectedError unless connected?
    @lock.synchronize do
      @count ||= 0
      @count += 1
      if @count > 500
        @count = 0
        @current_connection = nil
      end
      @current_connection ||= @connections.sample
    end
  end
end

require 'cql/client/client'

Cql::Client::SynchronousClient.class_eval do
  def current_connection
    async.instance_variable_get(:@connection_manager).current_connection
  end
end
