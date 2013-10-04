module DatastaxRails
  module Cql
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