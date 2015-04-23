# require 'datastax_rails/rsolr_client_wrapper'
require 'rsolr/client_cert'
require 'rest_client'
require 'cassandra'

module DatastaxRails
  # The connection module holds all the code for establishing and maintaining a connection to
  # Datastax Exterprise.  This includes both the Cassandra and Solr connections.
  module Connection
    extend ActiveSupport::Concern

    included do
      include DatastaxRails::StatementCache
      class_attribute :connection
      class_attribute :cluster
      class_attribute :solr
      cattr_accessor :current_server
    end

    module ClassMethods # rubocop:disable Style/Documentation
      DEFAULT_OPTIONS = {
        servers:             ['127.0.0.1'],
        port:                9160,
        connection_options:  { timeout: 10 },
        ssl:                 false,
        server_max_requests: 500
      }

      # Establish a Cassandra connection to DSE.  datastax.yml will be read and the current environment's
      # settings passed to this method.
      #
      # The following is an example production configuration document.  Assume that your setup consists
      # of three datacenters each with three servers and RF=3 (i.e., you're storing your data 9 times)
      #
      #   servers: ["10.1.2.5"]
      #   port: 9042
      #   ssl:
      #     cert: config/datastax_rails.crt
      #     key: config/datastax_rails.key
      #     ca_cert: config/ca.crt
      #     keypass: changeme
      #   keyspace: "datastax_rails_production"
      #   strategy_class: "org.apache.cassandra.locator.NetworkTopologyStrategy"
      #   strategy_options: {"DS1": "3", "DS2": "3", "DS3": "3"}
      #   connection_options:
      #     timeout: 10
      #   solr:
      #     port: 8983
      #     path: /solr
      #
      # The +servers+ entry should be a list of all seed nodes for servers you wish to connect to.  DSR
      # will automatically connect to all nodes in the cluster or in the datacenter if you are using multiple
      # datacenters.  You can safely just list all nodes in a particular datacenter if you would like.
      #
      # The port to connect to, this port will be used for all nodes. Because the `system.peers` table does
      # not contain the port that the nodes are listening on, the port must be the same for all nodes.
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
      # * *retries* - Number of times a request will be retried. Should likely be the number of servers - 1.
      #   Defaults to 0.
      # * *server_retry_period* - Amount of time to wait before retrying a down server. Defaults to 1.
      # * *server_max_requests* - Number of requests to make to a server before moving to the next one (helps
      #   keep load balanced). Default to 500.
      # * *retry_overrides* - Overrides retries option for individual exceptions.
      # * *connect_timeout* - The connection timeout on the Thrift socket. Defaults to 0.1.
      # * *timeout* - The timeout for the transport layer. Defaults to 1.
      #
      # See +solr_connection+ for a description of the solr options in datastax.yml
      def establish_connection(spec)
        DatastaxRails::Base.config = spec.with_indifferent_access
        spec.reverse_merge!(DEFAULT_OPTIONS)
        load_balancing_policy =
          DatastaxRails::LoadBalancing::Policies::StickyDcAwareRoundRobin.new(spec[:server_max_requests])
        cluster_options = { hosts:                 spec[:servers],
                            connection_timeout:    spec[:connection_options][:timeout],
                            timeout:               spec[:connection_options][:timeout],
                            load_balancing_policy: load_balancing_policy }
        if ssl_type
          ca_cert = Pathname.new(DatastaxRails::Base.config[:ssl][:ca_cert])
          ca_cert = Rails.root.join(ca_cert) unless ca_cert.absolute?
          ssl_context = OpenSSL::SSL::SSLContext.new
          ssl_context.verify_mode = OpenSSL::SSL::VERIFY_PEER
          ssl_context.ca_file = ca_cert.to_s
          if ssl_type == :two_way_ssl
            cert = Pathname.new(DatastaxRails::Base.config[:ssl][:cert])
            key = Pathname.new(DatastaxRails::Base.config[:ssl][:key])
            pass = DatastaxRails::Base.config[:ssl][:keypass]
            cert = Rails.root.join(cert) unless cert.absolute?
            key = Rails.root.join(key) unless key.absolute?
            if pass
              ssl_context.key = OpenSSL::PKey::RSA.new(key.read, pass)
            else
              ssl_context.key = OpenSSL::PKey::RSA.new(key.read)
            end
            ssl_context.cert = OpenSSL::X509::Certificate.new(cert.read)
          end
          cluster_options[:ssl] = ssl_context
        end

        self.current_server = spec[:servers].first

        self.cluster = Cassandra.cluster(cluster_options)
        self.connection = cluster.connect(spec[:keyspace])
      end

      def ssl_type
        return false unless DatastaxRails::Base.config && DatastaxRails::Base.config[:ssl]
        config = DatastaxRails::Base.config
        if config[:ssl][:key] && config[:ssl][:cert]
          :two_way_ssl
        elsif config[:ssl][:ca_cert]
          :one_way_ssl
        else
          false
        end
      end

      # rubocop:disable Style/RescueModifier
      def reconnect
        connection.close rescue true
        self.connection = nil
        establish_connection(DatastaxRails::Base.config)
      end

      # Returns the base portion of the URL for connecting to SOLR based on the current Cassandra server.
      #
      # @return [String] in the form of 'http://localhost:8983/solr'
      def solr_base_url
        DatastaxRails::Base.establish_connection unless connection
        port = DatastaxRails::Base.config[:solr][:port]
        path = DatastaxRails::Base.config[:solr][:path]
        protocol = ssl_type ? 'https' : 'http'
        "#{protocol}://#{current_server}:#{port}#{path}"
      end

      # Wraps and caches a solr connection object
      #
      # @params [Boolean] reconnect force a new connection
      # @return [DatastaxRails::RSolrClientWrapper] a wrapped RSolr connection
      def solr_connection(reconnect = false)
        if !@rsolr || reconnect
          @rsolr = DatastaxRails::RSolrClientWrapper.new(establish_solr_connection, self)
        end
        @rsolr
      end

      # Similar to +establish_connection+, this method creates a connection object for Solr. Since HTTP is stateless,
      # this doesn't actually launch the connection, but it gets everything set up so that RSolr can do its work. It's
      # important to note that unlike the cassandra connection which is global to all of DSR, each model will have its
      # own solr_connection.
      #
      # @return [RSolr::Client] RSolr client object
      def establish_solr_connection
        opts = { url: "#{solr_base_url}/#{DatastaxRails::Base.connection.keyspace}.#{column_family}" }
        if ssl_type == :two_way_ssl
          ca_cert = Pathname.new(DatastaxRails::Base.config[:ssl][:ca_cert])
          cert = Pathname.new(DatastaxRails::Base.config[:ssl][:cert])
          key = Pathname.new(DatastaxRails::Base.config[:ssl][:key])
          pass = DatastaxRails::Base.config[:ssl][:keypass]
          ca_cert = Rails.root.join(ca_cert) unless ca_cert.absolute?
          cert = Rails.root.join(cert) unless cert.absolute?
          key = Rails.root.join(key) unless key.absolute?
          opts[:ssl_cert_file] = cert.to_s
          opts[:ssl_key_file] = key.to_s
          opts[:ssl_key_pass] = pass if pass
          opts[:ssl_ca_file] = ca_cert.to_s

          RSolr::ClientCert.connect opts
        else
          RSolr.connect opts
        end
      end
    end
  end
end
