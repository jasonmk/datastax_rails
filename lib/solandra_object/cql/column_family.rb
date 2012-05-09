module DatastaxRails
  module Cql
    class ColumnFamily
      def initialize(klass)
        @klass = klass
      end
      
      def create_column_family
        DatastaxRails::Cql::CreateColumnFamily.new(@klass.column_family)
      end
      
      def delete(*keys)
        DatastaxRails::Cql::Delete.new(@klass, keys.flatten)
      end
      
      def insert
        DatastaxRails::Cql::Insert.new(@klass)
      end
      
      def drop_column_family
        DatastaxRails::Cql::DropColumnFamily.new(@klass.column_family)
      end
      
      def select(*columns)
        columns << "*" if columns.empty?
        DatastaxRails::Cql::Select.new(@klass, columns.flatten)
      end

      def truncate
        DatastaxRails::Cql::Truncate.new(@klass)
      end
      
      def update(*keys)
        DatastaxRails::Cql::Update.new(@klass, keys.flatten)
      end
    end
  end
end