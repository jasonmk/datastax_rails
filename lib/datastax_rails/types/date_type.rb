module DatastaxRails
  module Types
    class DateType < BaseType
      DEFAULTS = {:solr_type => 'date', :indexed => :solr, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false, :cassandra_type => 'timestamp'}
      FORMAT = '%Y-%m-%dT%H:%M:%SZ'

      def encode(value)
        return unless value
        raise ArgumentError.new("#{self} requires a Date") unless value.kind_of?(Date) || value.kind_of?(Time)
        value.to_date.strftime(FORMAT)
      end

      def decode(str)
        return str if str.kind_of?(Date)
        Date.parse(str) rescue nil
      end
      
      def full_solr_range
        '[* TO *]'
      end
    end
  end
end
