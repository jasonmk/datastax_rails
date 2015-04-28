module DatastaxRails
  module Cql
    # CQL generation for ALTER COLUMNFAMILY
    class AlterColumnFamily < Base
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

      def rename(col1, col2)
        set_column([col1, col2])
        @action = 'RENAME'
        self
      end

      def set_column(column) # rubocop:disable Style/AccessorMethodName
        fail ArgumentError, 'Only one operation allowed per CQL call' if @action
        @column = column
      end

      def to_cql
        stmt = "ALTER COLUMNFAMILY #{@cf_name} "
        if (@action == 'ALTER')
          stmt << "ALTER #{@column.keys.first} TYPE #{@column.values.first}"
        elsif (@action == 'ADD')
          stmt << "ADD #{@column.keys.first} #{@column.values.first}"
        elsif (@action == 'DROP')
          stmt << "DROP #{@column}"
        elsif (@action == 'RENAME')
          stmt << "RENAME \"#{@column[0]}\" TO \"#{@column[1]}\""
        end

        stmt
      end
    end
  end
end
