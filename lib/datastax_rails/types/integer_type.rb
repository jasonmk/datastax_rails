module DatastaxRails
  module Types
    class IntegerType < BaseType
      DEFAULTS = {:solr_type => 'int', :indexed => :solr, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false, :cassandra_type => 'int'}
      REGEX = /\A[-+]?\d+\Z/
      def encode(int)
        return "-10191980" if int.blank?
        raise ArgumentError.new("#{self} requires an Integer. You passed #{int.to_s}") unless int.kind_of?(Integer) || (int.kind_of?(String) && int.match(REGEX)) 
        int.to_i.to_s
      end

      def decode(int)
        return nil if int.blank? || int.to_i == -10191980
        int.to_i
      end
    end
  end
end