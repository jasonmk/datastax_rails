module DatastaxRails
  module Types
    class StringType < BaseType
      DEFAULTS = {:solr_type => 'string', :indexed => :solr, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => true, :cassandra_type => 'text'}
      def encode(str)
        str = "" unless str
        str.to_s
      end

      def wrap(record, name, value)
        txt = (value.frozen? ? value.to_s.dup : value)
        txt.respond_to?(:force_encoding) ? txt.force_encoding('UTF-8') : txt
      end
    end
  end
end