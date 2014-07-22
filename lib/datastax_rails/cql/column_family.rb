module DatastaxRails
  module Cql
    # Helper class to create CQL-building objects
    class ColumnFamily
      def initialize(klass)
        @klass = klass
      end

      def create_column_family
        DatastaxRails::Cql::CreateColumnFamily.new(@klass.column_family)
      end

      def delete(key)
        DatastaxRails::Cql::Delete.new(@klass, key)
      end

      def insert
        DatastaxRails::Cql::Insert.new(@klass)
      end

      def drop_column_family
        DatastaxRails::Cql::DropColumnFamily.new(@klass.column_family)
      end

      def select(*columns)
        columns << '*' if columns.empty?
        DatastaxRails::Cql::Select.new(@klass, columns.flatten)
      end

      def truncate
        DatastaxRails::Cql::Truncate.new(@klass)
      end

      def update(key)
        DatastaxRails::Cql::Update.new(@klass, key)
      end
    end
  end
end
