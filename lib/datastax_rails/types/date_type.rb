module DatastaxRails
  module Types
    class DateType < BaseType
      DEFAULTS = {:solr_type => 'date', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      FORMAT = '%Y-%m-%dT%H:%M:%SZ'

      def encode(value)
        raise ArgumentError.new("#{self} requires a Date") unless value.to_date
        value.to_date.strftime(FORMAT)
      end

      def decode(str)
        Date.parse(str) rescue nil
      end
    end
  end
end
