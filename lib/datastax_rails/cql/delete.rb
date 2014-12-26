module DatastaxRails
  module Cql
    # Generates CQL to delete a record from Cassandra
    class Delete < Base
      def initialize(klass, key)
        @klass = klass
        @key = key
        @timestamp = nil
        @columns = []
        super
      end

      def columns(columns)
        @columns = columns
        self
      end

      def timestamp(timestamp)
        @timestamp = timestamp
        self
      end

      def to_cql
        @values = []
        stmt = "DELETE #{@columns.join(',')} FROM #{@klass.column_family} "
        stmt << "AND TIMESTAMP #{@timestamp} " if @timestamp
        conditions = []

        @key.each do |col, val|
          conditions << "\"#{col}\" = ?"
          @values << val
        end

        stmt << "WHERE #{conditions.join(' AND ')}"

        stmt
      end

      include Transactions
    end
  end
end
