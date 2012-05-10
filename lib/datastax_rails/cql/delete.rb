module DatastaxRails
  module Cql
    class Delete < Base
      def initialize(klass, keys)
        @klass = klass
        @keys = keys
        @consistency = DatastaxRails::Cql::Consistency::QUORUM
        @timestamp = nil
        @columns = []
      end
      
      def using(consistency)
        @consistency = consistency
        self
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
        values = []
        stmt = "DELETE #{@columns.join(',')} FROM #{@klass.column_family} USING CONSISTENCY #{@consistency} "
        
        if(@timestamp)
          stmt << "AND TIMESTAMP #{@timestamp} "
        end
        
        stmt << "WHERE KEY IN (?)"
        
        CassandraCQL::Statement.sanitize(stmt, keys)
      end
    end
  end
end