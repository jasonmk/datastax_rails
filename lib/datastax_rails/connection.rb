module DatastaxRails
  module Connection
    extend ActiveSupport::Concern
    
    included do
      class_attribute :connection
    end

    module ClassMethods
      DEFAULT_OPTIONS = {
        :servers => "127.0.0.1:9160",
        :thrift => {}
      }
      def establish_connection(spec)
        DatastaxRails::Base.config = spec.with_indifferent_access
        spec.reverse_merge!(DEFAULT_OPTIONS)
        connection_options = spec[:connection_options] || {}
        self.connection = CassandraCQL::Database.new(spec[:servers], {:keyspace => spec[:keyspace]}, connection_options.symbolize_keys)
      end
    end
  end
end
