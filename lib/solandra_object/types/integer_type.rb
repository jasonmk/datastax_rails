module DatastaxRails
  module Types
    class IntegerType < BaseType
      DEFAULTS = {:solr_type => 'int', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      REGEX = /\A[-+]?\d+\Z/
      def encode(int)
        raise ArgumentError.new("#{self} requires an Integer. You passed #{int.inspect}") unless int.kind_of?(Integer)
        int.to_s
      end

      def decode(str)
        return nil if str.empty?
        return nil unless str.kind_of?(String) && str.match(REGEX)
        str.to_i
      end
    end
  end
end