module DatastaxRails
  module Types
    class TimeType < BaseType
      DEFAULTS = {:solr_type => 'date', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      # lifted from the implementation of Time.xmlschema and simplified
      REGEX = /\A\s*
                (-?\d+)-(\d\d)-(\d\d)
                T
                (\d\d):(\d\d):(\d\d)
                (\.\d*)?
                (Z|[+-]\d\d:\d\d)?
                \s*\z/ix

      def encode(time)
        raise ArgumentError.new("#{self} requires a Time") unless time.kind_of?(Time)
        time.xmlschema(6)
      end

      def decode(str)
        return nil if str.empty?
        return nil unless str.kind_of?(String) && str.match(REGEX)
        Time.xmlschema(str)
      end
    end
  end
end