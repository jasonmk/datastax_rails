module DatastaxRails#:nodoc:
  module Cql #:nodoc:
    class CreateColumnFamily < Base #:nodoc:
      def initialize(cf_name)
        @cf_name = cf_name
        @columns = {}
        @storage_parameters = []
        @key_type = 'uuid'
        @key_columns = @key_name = "key"
      end
      
      def key_type(key_type)
        @key_type = key_type
        self
      end
      
      def key_name(key_name)
        @key_name = key_name
        self
      end
      
      def key_columns(key_columns)
        @key_columns = key_columns
        self
      end
      
      def with(with)
        @storage_parameters << with
        self
      end
      
      def columns(columns)
        @columns.merge! columns
        self
      end
      
      # Migration helpers
      def comment=(comment)
        with("comment" => comment)
      end
      
      def comparator=(comp)
        with("comparator" => comp)
      end
      
      def default_validation=(val)
        with("default_validation" => val)
      end
      
      def to_cql
        stmt = "CREATE COLUMNFAMILY #{@cf_name} (#{@key_name} #{@key_type}, "
        @columns.each do |name,type|
          stmt << "#{name} #{type}, "
        end
        stmt << "PRIMARY KEY (#{@key_columns}))"
        unless @storage_parameters.empty?
          stmt << " WITH "
          stmt << @storage_parameters.join(" AND ")
        end
        
        stmt
      end
    end
  end
end