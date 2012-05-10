module DatastaxRails
  module Types
    class FloatType < BaseType
      DEFAULTS = {:solr_type => 'float', :indexed => true, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false}
      REGEX = /\A[-+]?(\d+(\.\d+)?|\.\d+)\Z/
      def encode(float)
        raise ArgumentError.new("#{self} requires a Float") unless float.kind_of?(Float)
        float.to_s
      end

      def decode(str)
        return nil if str.empty?
        return nil unless str.kind_of?(String) && str.match(REGEX)
        str.to_f
      end
    end
  end
end