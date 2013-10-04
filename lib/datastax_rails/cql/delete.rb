module DatastaxRails
  module Cql
    class Delete < Base
      def initialize(klass, keys)
        @klass = klass
        @keys = keys
        @timestamp = nil
        @columns = []
        @conditions = {}
        @key_name = "key"
        super
      end
      
      def using(consistency)
        @consistency = consistency
        self
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
        values = [@keys.collect{|k|k.to_s}]
        stmt = "DELETE #{@columns.join(',')} FROM #{@klass.column_family} "
        
        if(@timestamp)
          stmt << "AND TIMESTAMP #{@timestamp} "
        end
        
        stmt << "WHERE \"#{@key_name}\" IN (?)"
        
        @conditions.each do |col,val|
          stmt << " AND #{col} = ?"
          values << val
        end
        
        CassandraCQL::Statement.sanitize(stmt, values)
      end
    end
  end
end