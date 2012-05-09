module DatastaxRails
  module Types
    class DateType < BaseType
      DEFAULTS = {:solr_type => 'date', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      FORMAT = '%Y-%m-%d'
      REGEX = /\A\d{4}-\d{2}-\d{2}\Z/

      def encode(value)
        raise ArgumentError.new("#{self} requires a Date") unless value.kind_of?(Date)
        value.strftime(FORMAT)
      end

      def decode(str)
        Date.parse(str) rescue nil
      end
    end
  end
end
