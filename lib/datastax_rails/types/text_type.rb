module DatastaxRails
  module Types
    class TextType < BaseType
      DEFAULTS = {:solr_type => 'text', :indexed => :solr, :stored => true, :multi_valued => false, :sortable => false, :tokenized => true, :fulltext => true, :cassandra_type => 'text'}
      def encode(str)
        str.to_s.dup
      end

      def wrap(record, name, value)
        txt = (value.frozen? ? value.dup : value)
        txt.respond_to?(:force_encoding) ? txt.force_encoding('UTF-8') : txt
      end
    end
  end
end
