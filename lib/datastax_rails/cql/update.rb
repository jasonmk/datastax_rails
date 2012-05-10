module DatastaxRails
  module Cql
    class Update < Base
      def initialize(klass, key)
        @klass = klass
        @key = key
        @consistency = DatastaxRails::Cql::Consistency::LOCAL_QUORUM
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
        values = []
        
        stmt = "update #{@klass.column_family} using consistency #{@consistency} "
        
        if(@ttl)
          stmt << "AND TTL #{@ttl} "
        end
        
        if(@timestamp)
          stmt << "AND TIMESTAMP #{@timestamp}"
        end
        
        stmt << "SET "
        
        first_entry = @columns.shift
        
        stmt << "#{first_entry.first.to_s} = ? "
        values << first_entry.last
        
        @columns.each do |k,v|
          stmt << ", #{k.to_s} = ? "
          values << v
        end
        
        stmt << "WHERE KEY IN (?)"
        values << @key
        
        CassandraCQL::Statement.sanitize(stmt, values)
      end
    end
  end
end