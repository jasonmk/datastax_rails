module DatastaxRails
  module Types
    class BinaryType < BaseType
      DEFAULTS = {:solr_type => false, :indexed => false, :stored => false, :multi_valued => false, :sortable => false, :tokenized => false, :fulltext => false, :cassandra_type => 'blob'}
      def encode(str)
        raise ArgumentError.new("#{self} requires a String") unless str.kind_of?(String)
        Base64.encode64(str)
      end
      
      def decode(str)
        Base64.decode64(str)
      end

      def wrap(record, name, value)
        (value.frozen? ? value.dup : value)
      end
    end
  end
end