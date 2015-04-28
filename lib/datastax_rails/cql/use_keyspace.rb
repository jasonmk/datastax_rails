module DatastaxRails
  module Cql
    # CQL generation for USE KEYSPACE
    # XXX: Is this still used anywhere?
    class UseKeyspace < Base
      def initialize(ks_name)
        @ks_name = ks_name
      end

      def to_cql
        "USE #{@ks_name}"
      end
    end
  end
end
