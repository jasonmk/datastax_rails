module DatastaxRails#:nodoc:
  module Cql #:nodoc:
    class CreateColumnFamily < Base #:nodoc:
      def initialize(cf_name)
        @cf_name = cf_name
        @columns = {}
        @storage_parameters = []
        @primary_key = 'id'
      end

      def primary_key(pk)
        @primary_key = pk
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
        stmt = "CREATE COLUMNFAMILY #{@cf_name} ("
        @columns.each do |name,type|
          stmt << "#{name} #{type}, "
        end
        stmt << "PRIMARY KEY (#{@primary_key}))"
        unless @storage_parameters.empty?
          stmt << " WITH "
          stmt << @storage_parameters.join(" AND ")
        end
        
        stmt
      end
    end
  end
end