module DatastaxRails
  module Cql
    class Base
      # Base initialize that sets the default consistency.
      def initialize(klass, *args)
        @consistency = klass.default_consistency.to_s.upcase
      end

      # Abstract.  Should be overridden by subclasses
      def to_cql
        raise NotImplementedError
      end
      
      # Generates the CQL and calls Cassandra to execute it.
      # If you are using this outside of Rails, then DatastaxRails::Base.connection must have
      # already been set up (Rails does this for you).
      def execute
        cql = self.to_cql
        Rails.logger.debug(cql)
        DatastaxRails::Base.connection.execute_cql_query(cql)
      end
    end
  end
end