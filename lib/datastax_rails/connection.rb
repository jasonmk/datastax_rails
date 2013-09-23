# require 'datastax_rails/rsolr_client_wrapper'
require 'rsolr/client_cert'
require 'rest_client'
require "cassandra-cql/1.2"
module DatastaxRails
  # The connection module holds all the code for establishing and maintaining a connection to
  # Datastax Exterprise.  This includes both the Cassandra and Solr connections.
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection
      class_attribute :solr
    end

    module ClassMethods
      DEFAULT_OPTIONS = {
        :servers => "127.0.0.1:9160",
        :thrift => {},
        :cql_version => '3.0.0'
      }
      
      # Returns the current server that we are talking to.  This is useful when you are talking to a
      # cluster, and we want to know which server specifically we are connected to.
      #
      # Used by Relation to calculate the SOLR URL so that it follows the Cassandra connection.
      #
      # @return [String] the hostname or ip address of the current server
      def current_server
        thrift_client.instance_variable_get(:@current_server).to_s.split(/\:/).first
      end
      
      # Returns the thrift client object
      def thrift_client
        self.connection.instance_variable_get(:@connection)
      end
      
      # Establish a Cassandra connection to DSE.  datastax.yml will be read and the current environment's
      # settings passed to this method.
      #
      # The following is an example production configuration document.  Assume that your setup consists
      # of three datacenters each with three servers and RF=3 (i.e., you're storing your data 9 times)
      #
      #   servers: ["10.1.2.5:9160", "10.1.2.6:9160", "10.1.2.7:9160"]
      #   keyspace: "datastax_rails_production"
      #   strategy_class: "org.apache.cassandra.locator.NetworkTopologyStrategy"
      #   strategy_options: {"DS1": "3", "DS2": "3", "DS3": "3"} 
      #   connection_options:
      #     timeout: 10
      #     retries: 2
      #     server_max_requests: 1000
      #   solr:
      #     port: 8983
      #     path: /solr
      #     ssl:
      #       use_ssl: true
      #       cert: config/datastax_rails.crt
      #       key: config/datastax_rails.key
      #       keypass: changeme
      #
      # The +servers+ entry should be a list of all of the servers in your local datacenter.  These
      # are the servers that DSR will attempt to connect to and will round-robin through.
      #
      # Since we're using the NetworkTopologyStrategy for our locator, it is important that you configure
      # cassandra-topology.properties.  See the DSE documentation at http://www.datastax.com for more
      # information.
      #
      # strategy_options lets us specify what our topology looks like.  In this case, we have RF=3 in all
      # three of our datacenters (DS1, DS2, and DS3).
      #
      # connection_options are the options that are passed to the thrift layer for the connection to
      # cassandra.
      # * *retries* - Number of times a request will be retried. Should likely be the number of servers - 1. Defaults to 0.
      # * *server_retry_period* - Amount of time to wait before retrying a down server. Defaults to 1.
      # * *server_max_requests* - Number of requests to make to a server before moving to the next one (helps keep load balanced). Default to nil which means cycling does not take place.
      # * *retry_overrides* - Overrides retries option for individual exceptions.
      # * *connect_timeout* - The connection timeout on the Thrift socket. Defaults to 0.1.
      # * *timeout* - The timeout for the transport layer. Defaults to 1.
      # * *timeout_overrides* - Overrides the timeout value for specific methods (advanced).
      # * *exception_classes* - List of exceptions for which Thrift will automatically retry a new server in the cluster (up to retry limit).
      #   Defaults to [IOError, Thrift::Exception, Thrift::ApplicationException, Thrift::TransportException].
      # * *exception_class_overrides* - List of exceptions which will never cause a retry. Defaults to [CassandraCQL::Thrift::InvalidRequestException].
      # * *wrapped_exception_options* - List of exceptions that will be automatically wrapped in an exception provided by client class with the same name (advanced).
      #   Defaults to [Thrift::ApplicationException, Thrift::TransportException].
      # * *raise* - Whether to raise exceptions or default calls that cause an error (advanced). Defaults to true (raise exceptions).
      # * *defaults* - When raise is false and an error is encountered, these methods are called to default the return value (advanced). Should be a hash of method names to values.
      # * *protocol* - The thrift protocol to use (advanced). Defaults to Thrift::BinaryProtocol.
      # * *protocol_extra_params* - Any extra parameters to send to the protocol (advanced).
      # * *transport* - The thrift transport to use (advanced). Defaults to Thrift::Socket.
      # * *transport_wrapper* - The thrift transport wrapper to use (advanced). Defaults to Thrift::FramedTransport.
      #
      # See +solr_connection+ for a description of the solr options in datastax.yml
      def establish_connection(spec)
        DatastaxRails::Base.config = spec.with_indifferent_access
        spec.reverse_merge!(DEFAULT_OPTIONS)
        connection_options = spec[:connection_options] || {}
        self.connection = CassandraCQL::Database.new(spec[:servers], {:keyspace => spec[:keyspace], :cql_version => spec[:cql_version]}, connection_options.symbolize_keys)
      end
      
      # Returns the base portion of the URL for connecting to SOLR based on the current Cassandra server.
      #
      # @return [String] in the form of 'http://localhost:8983/solr'
      def solr_base_url
        DatastaxRails::Base.establish_connection unless self.connection
        port = DatastaxRails::Base.config[:solr][:port]
        path = DatastaxRails::Base.config[:solr][:path]
        protocol = DatastaxRails::Base.config[:solr].has_key?(:ssl) && DatastaxRails::Base.config[:solr][:ssl][:use_ssl] ? 'https' : 'http'
        "#{protocol}://#{self.current_server}:#{port}#{path}"
      end
      
      # Wraps and caches a solr connection object
      #
      # @params [Boolean] reconnect force a new connection
      # @return [DatastaxRails::RSolrClientWrapper] a wrapped RSolr connection      
      def solr_connection(reconnect = false)
        if(!@rsolr || reconnect)
          @rsolr = DatastaxRails::RSolrClientWrapper.new(establish_solr_connection, self)
        end
        @rsolr
      end
      
      # Similar to +establish_connection+, this method creates a connection object for Solr.  Since HTTP is stateless, this doesn't
      # actually launch the connection, but it gets everything set up so that RSolr can do its work.  It's important to note that
      # unlike the cassandra connection which is global to all of DSR, each model will have its own solr_connection.
      #
      # @return [RSolr::Client] RSolr client object
      def establish_solr_connection
        opts = {:url => "#{solr_base_url}/#{DatastaxRails::Base.connection.keyspace}.#{self.column_family}"}
        if DatastaxRails::Base.config[:solr].has_key?(:ssl) && 
            DatastaxRails::Base.config[:solr][:ssl].has_key?(:cert) && 
            DatastaxRails::Base.config[:solr][:ssl][:use_ssl]
          cert = Pathname.new(DatastaxRails::Base.config[:solr][:ssl][:cert])
          key = Pathname.new(DatastaxRails::Base.config[:solr][:ssl][:key])
          pass = DatastaxRails::Base.config[:solr][:ssl][:keypass]
          cert = Rails.root.join(cert) unless cert.absolute?
          key = Rails.root.join(key) unless key.absolute?
          opts[:ssl_cert_file] = cert.to_s
          opts[:ssl_key_file] = key.to_s
          opts[:ssl_key_pass] = pass if pass
          
          RSolr::ClientCert.connect opts
        else
          RSolr.connect opts
        end
      end
    end
  end
end
