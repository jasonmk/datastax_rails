module DatastaxRails
  module Types
    class TimeWithZoneType < BaseType
      DEFAULTS = {:solr_type => 'date', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      def encode(time)
        raise ArgumentError.new("#{self} requires a Time") unless time.kind_of?(Time)
        time.utc.xmlschema(6)
      end

      def decode(str)
        return nil if str.empty?
        return nil unless str.kind_of?(String) && str.match(TimeType::REGEX)
        Time.xmlschema(str).in_time_zone
      end
    end
  end
end