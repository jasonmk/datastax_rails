module DatastaxRails
  module Cql
    # CQL generation for DROP INDEX
    class DropIndex < Base
      def initialize(index_name)
        @index_name = index_name
      end

      def to_cql
        "DROP INDEX #{@index_name}"
      end
    end
  end
end
