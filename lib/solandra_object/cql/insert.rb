module DatastaxRails
  module Cql
    class Insert < Base
      def initialize(klass)
        @klass = klass
        @consistency = DatastaxRails::Cql::Consistency::LOCAL_QUORUM
        @ttl = nil
        @timestamp = nil
        @columns = {}
      end
      
      def using(consistency)
        @consistency = consistency
        self
      end
      
      def columns(columns)
        @columns.merge!(columns)
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
        values = []
        keys = []
        @columns.each do |k,v|
          keys << k.to_s
          values << v
        end
        stmt = "INSERT INTO #{@klass.column_family} (#{keys.join(',')}) VALUES (#{('?'*keys.size).split(//).join(',')}) USING CONSISTENCY #{@consistency} "
        
        if(@ttl)
          stmt << "AND TTL #{@ttl} "
        end
        
        if(@timestamp)
          stmt << "AND TIMESTAMP #{@timestamp}"
        end
        
        CassandraCQL::Statement.sanitize(stmt, values)
      end
    end
  end
end