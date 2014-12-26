module DatastaxRails
  module Cql
    class Update < Base
      def initialize(klass, key)
        @klass = klass
        @key = key
        @columns = {}
        super
      end

      def columns(columns)
        @columns.merge!(columns)
        self
      end

      def limit(limit)
        @limit = limit
        self
      end

      def ttl(ttl)
        @ttl = ttl
        self
      end

      def timestamp(timestamp)
        @timestamp = timestamp
        self
      end

      def to_cql
        stmt = "update #{@klass.column_family} "
        stmt << "AND TTL #{@ttl} " if @ttl
        stmt << "AND TIMESTAMP #{@timestamp}" if @timestamp

        unless @columns.empty?
          stmt << 'SET '
          updates = []
          @columns.each do |k, v|
            @values << v
            updates << "\"#{k}\" = ?"
          end

          stmt << updates.join(', ')
        end
        conditions = []
        @key.each do |k, v|
          conditions << "\"#{k}\" = ?"
          @values << v
        end
        stmt << " WHERE #{conditions.join(' AND ')}"
      end
      include Transactions
    end
  end
end
