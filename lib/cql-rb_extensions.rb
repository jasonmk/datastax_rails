require 'cql'
require 'cql/client/connection_manager'

Cql::Client::ConnectionManager.class_eval do
  attr_reader :current_connection
  
  def random_connection
    raise NotConnectedError unless connected?
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

require 'cql/client/synchronous_client'

Cql::Client::SynchronousClient.class_eval do
  def current_connection
    async.instance_variable_get(:@connection_manager).current_connection
  end
end