module DatastaxRails
  module Cql
    # CQL generation for UPDATE
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
        if @ttl || @timestamp
          stmt << 'USING '
          stmt << "TTL #{@ttl} " if @ttl
          stmt << 'AND ' if @ttl && @timestamp
          stmt << "TIMESTAMP #{@timestamp} " if @timestamp
        end

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
