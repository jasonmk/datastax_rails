module DatastaxRails
  module Identity
    class NaturalKeyFactory < AbstractKeyFactory
      class NaturalKey
        attr_reader :value

        delegate :size, :bytesize, :to => :value
        def initialize(value)
          @value = value
        end

        def to_s
          value
        end

        def ==(other)
          other.to_s == to_s
        end
      end

      attr_reader :attributes, :separator

      def initialize(options)
        @attributes = [*options[:attributes]]
        @separator  = options[:separator] || "-"
        @key_columns = options[:column].blank? ? @attributes.dup : Array.wrap(options[:column])
      end

      def next_key(object)
        NaturalKey.new(attributes.map { |a| object.attributes[a.to_s] }.join(separator))
      end

      def parse(paramized_key)
        NaturalKey.new(paramized_key)
      end
    end
  end
end

