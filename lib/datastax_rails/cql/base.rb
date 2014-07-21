module DatastaxRails
  module Cql
    class Base
      # Base initialize that sets the default consistency.
      def initialize(klass, *_args)
        @consistency = klass.default_consistency.to_s.downcase.to_sym
        @keyspace = DatastaxRails::Base.config[:keyspace]
        @values = []
      end

      def using(consistency)
        @consistency = consistency.to_s.downcase.to_sym
        self
      end

      def key_name
        @klass.key_factory.key_columns
      end

      # Abstract. Should be overridden by subclasses
      def to_cql
        fail NotImplementedError
      end

      # Generates the CQL and calls Cassandra to execute it.
      # If you are using this outside of Rails, then DatastaxRails::Base.connection must have
      # already been set up (Rails does this for you).
      def execute
        cql = to_cql
        puts cql if ENV['DEBUG_CQL'] == 'true'
        pp @values if ENV['DEBUG_CQL'] == 'true'
        digest = Digest::MD5.digest cql
        stmt = DatastaxRails::Base.statement_cache[digest] ||= DatastaxRails::Base.connection.prepare(cql)
        if @consistency
          stmt.execute(*@values, consistency: @consistency)
        else
          stmt.execute(*@values)
        end
      end
    end
  end
end
