module DatastaxRails
  module Types
    class StringType < BaseType
      DEFAULTS = {:solr_type => 'string', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      def encode(str)
        raise ArgumentError.new("#{self} requires a String") unless str.kind_of?(String)
        str.dup
      end

      def wrap(record, name, value)
        txt = (value.frozen? ? value.dup : value)
        txt.respond_to?(:force_encoding) ? txt.force_encoding('UTF-8') : txt
      end
    end
  end
end