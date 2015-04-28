module DatastaxRails
  module Cql
    # CQL generate for DROP COLUMNFAMILY
    class DropColumnFamily < Base
      def initialize(cf_name)
        @cf_name = cf_name
      end

      def to_cql
        "DROP COLUMNFAMILY #{@cf_name}"
      end
    end
  end
end
