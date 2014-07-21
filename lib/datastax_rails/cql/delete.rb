module DatastaxRails
  module Cql
    class Delete < Base
      def initialize(klass, keys)
        @klass = klass
        @keys = keys
        @timestamp = nil
        @columns = []
        @conditions = {}
        @key_name = @klass.primary_key
        super
      end

      def columns(columns)
        @columns = columns
        self
      end

      def conditions(conditions)
        @conditions.merge!(conditions)
        self
      end

      def timestamp(timestamp)
        @timestamp = timestamp
        self
      end

      def key_name(key_name)
        @key_name = key_name
        self
      end

      def to_cql
        @values = @keys
        stmt = "DELETE #{@columns.join(',')} FROM #{@klass.column_family} "
        stmt << "AND TIMESTAMP #{@timestamp} " if @timestamp
        stmt << "WHERE \"#{@key_name}\" IN (?)"

        @conditions.each do |col, val|
          stmt << " AND #{col} = ?"
          @values << val
        end

        stmt
      end
    end
  end
end
