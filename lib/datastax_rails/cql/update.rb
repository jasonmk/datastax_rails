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
        column_names = @columns.keys
        
        stmt = "update #{@klass.column_family} "
        
        if(@ttl)
          stmt << "AND TTL #{@ttl} "
        end
        
        if(@timestamp)
          stmt << "AND TIMESTAMP #{@timestamp}"
        end
        
        unless @columns.empty?
          stmt << "SET "
          updates = []
          @columns.each do |k,v|
            @values << v
            updates << "\"#{k}\" = ?"
          end
          
          stmt << updates.join(", ")
        end
        
        stmt << " WHERE #{@klass.primary_key} IN (?)"
        @values << @key
        stmt.force_encoding('UTF-8')
      end
    end
  end
end