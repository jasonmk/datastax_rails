module DatastaxRails
  module Types
    class TimeType < BaseType
      DEFAULTS = {:solr_type => 'date', :indexed => :solr, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false, :cassandra_type => 'timestamp'}
      FORMAT = "%Y-%m-%dT%H:%M:%SZ"

      def encode(time)
        return unless time
        raise ArgumentError.new("#{self} requires a Time") unless time.kind_of?(Time)
        time.utc.strftime(FORMAT)
      end

      def decode(str)
        return str if str.kind_of?(Time)
        Time.zone.parse(str) rescue nil
      end
      
      def full_solr_range
        '[* TO *]'
      end
    end
  end
end