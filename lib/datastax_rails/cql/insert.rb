module DatastaxRails
  module Cql
    class Insert < Base
      def initialize(klass)
        @klass = klass
        @ttl = nil
        @timestamp = nil
        @columns = {}
        super
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
        stmt = "INSERT INTO #{@klass.column_family} (#{keys.join(',')}) VALUES (#{('?'*keys.size).split(//).join(',')}) "
        
        if(@ttl)
          stmt << "AND TTL #{@ttl} "
        end
        
        if(@timestamp)
          stmt << "AND TIMESTAMP #{@timestamp}"
        end
        
        CassandraCQL::Statement.sanitize(stmt, values).force_encoding('UTF-8')
      end
    end
  end
end