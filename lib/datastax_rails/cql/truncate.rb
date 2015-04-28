module DatastaxRails
  module Cql
    # CQL generation for TRUNCATE
    class Truncate < Base
      def initialize(klass)
        @klass = klass
        super
      end

      def to_cql
        "TRUNCATE #{@klass.column_family}"
      end
    end
  end
end
