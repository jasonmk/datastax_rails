module DatastaxRails
  module Types
    class BooleanType < BaseType
      DEFAULTS = {:solr_type => 'boolean', :indexed => :solr, :stored => true, :multi_valued => false, :sortable => true, :tokenized => false, :fulltext => false, :cassandra_type => 'boolean'}
      TRUE_VALS = [true, 'true', '1', 'Y']
      FALSE_VALS = [false, 'false', '0', '', 'N', nil, 'null']
      VALID_VALS = TRUE_VALS + FALSE_VALS
      
      def encode(bool)
        unless VALID_VALS.include?(bool)
          raise ArgumentError.new("#{self} requires a boolean")
        end
        TRUE_VALS.include?(bool) ? '1' : '0'
      end

      def decode(str)
        raise ArgumentError.new("Cannot convert #{str} into a boolean") unless VALID_VALS.include?(str)
        TRUE_VALS.include?(str)
      end
    end
  end
end