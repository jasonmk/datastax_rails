module DatastaxRails
  module Types
    # All of the DSR type classes inherit from here.  It sets up some default options and doesn basic conversion
    # to strings.  Subclasses can override these methods as needed.
    #
    # NOTE: All subclasses MUST declare a +DEFAULTS+ constant that specifies the indexing defaults.  Defaults may of
    # course be overridden when the attribute is declared.
    class BaseType
      attr_accessor :options
      # Default initializer.  Sets the indexing options based on the DEFAULTS
      def initialize(options = {})
        @options = self.class::DEFAULTS.merge(options)
      end

      def default
        options[:default].dup if options[:default]
      end

      def encode(value)
        value.to_s
      end

      def decode(str)
        str
      end

      def wrap(record, name, value)
        value
      end
    end
  end
end