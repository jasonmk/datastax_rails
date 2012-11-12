module DatastaxRails#:nodoc:
  module Cql #:nodoc:
    class AlterColumnFamily < Base #:nodoc:
      def initialize(cf_name)
        @cf_name = cf_name
        @action = nil
      end
      
      def add(column)
        set_column(column)
        @action = 'ADD' 
        self
      end
      
      def drop(column)
        set_column(column)
        @action = 'DROP'
        self 
      end
      
      def alter(column)
        set_column(column)
        @action = 'ALTER'
        self 
      end

      def set_column(column)  
        if(@action)
          raise ArgumentError, "Only one operation allowed per CQL call"
        end
        @column = column
      end
      
      def to_cql
        stmt = "ALTER COLUMNFAMILY #{@cf_name} "
        if(@action == 'ALTER')
          stmt << "ALTER #{@column.keys.first} TYPE #{@column.values.first}"
        elsif(@action == 'ADD')
          stmt << "ADD #{@column.keys.first} #{@column.values.first}"
        elsif(@action == 'DROP')
          stmt << "DROP #{@column}"
        end
        
        stmt
      end
    end
  end
end