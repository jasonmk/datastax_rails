module DatastaxRails
  module Identity
    class CustomKeyFactory < AbstractKeyFactory
      class CustomKey
        attr_reader :value

        def initialize(value)
          @value = value
        end

        def to_s
          value
        end

        def ==(other)
          other.to_s == value
        end
      end

      attr_reader :method

      def initialize(options)
        @method = options[:method]
        @key_columns = Array.wrap(options[:column])
      end

      def next_key(object)
        CustomKey.new(object.send(@method))
      end

      def parse(value)
        value
      end
    end
  end
end

