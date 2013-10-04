module DatastaxRails
  module Cql
    class Update < Base
      def initialize(klass, key)
        @klass = klass
        @key = key
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
          
          first_entry = column_names.first
          
          stmt << CassandraCQL::Statement.sanitize("\"#{first_entry.to_s}\" = ?", [@columns[first_entry]])
          column_names[1..-1].each do |col|
            stmt << CassandraCQL::Statement.sanitize(", \"#{col.to_s}\" = ?", [@columns[col]])
          end
        end
        
        stmt << CassandraCQL::Statement.sanitize(" WHERE key IN (?)", [@key])
        stmt.force_encoding('UTF-8')
      end
      
      # def execute
        # puts to_cql.truncate(50)
      # end
    end
  end
end