module DatastaxRails#:nodoc:
  module Cql #:nodoc:
    class CreateColumnFamily < Base #:nodoc:
      def initialize(cf_name)
        @cf_name = cf_name
        @columns = {}
        @storage_parameters = {}
      end
      
      def with(with)
        @storage_parameters.merge!(with)
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
      
      def column_type=(type)
        # TODO: Ignored till CQL supports super-columns
      end
      
      def to_cql
        stmt = "CREATE COLUMNFAMILY #{@cf_name} (key uuid PRIMARY KEY"
        @columns.each do |name,type|
          stmt << ", #{name} #{type}"
        end
        stmt << ")"
        unless @storage_parameters.empty?
          stmt << " WITH "
          first_parm = @storage_parameter.shift
          stmt << "#{first_parm.first.to_s} = '#{first_parm.last.to_s}'"
          
          @storage_parameters.each do |key, value|
            stmt << " AND #{key.to_s} = '#{value.to_s}'"
          end
        end
        
        stmt
      end
    end
  end
end