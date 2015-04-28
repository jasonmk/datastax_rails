module DatastaxRails
  module Cql
    # CQL generation for DROP KEYSPACE
    class DropKeyspace < Base
      def initialize(ks_name)
        @ks_name = ks_name
      end

      def to_cql
        "DROP KEYSPACE #{@ks_name}"
      end
    end
  end
end
