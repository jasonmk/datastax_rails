module DatastaxRails
  module Cql
    class Truncate < Base
      def initialize(klass)
        @klass = klass
      end
      
      def to_cql
        "TRUNCATE #{@klass.column_family}"
      end
    end
  end
end