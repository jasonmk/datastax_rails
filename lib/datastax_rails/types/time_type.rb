module DatastaxRails
  module Types
    class TimeType < BaseType
      DEFAULTS = {:solr_type => 'date', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      FORMAT = "%Y-%m-%dT%H:%M:%SZ"

      def encode(time)
        raise ArgumentError.new("#{self} requires a Time") unless time.kind_of?(Time)
        time.strftime(FORMAT)
      end

      def decode(str)
        return str if str.kind_of?(Time)
        Time.parse(str) rescue nil
      end
    end
  end
end