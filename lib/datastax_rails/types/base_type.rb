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
        if options.has_key?(:default)
          options[:default].duplicable? ? options[:default].dup : options[:default]
        end
      end
      
      def encode(value, format = :solr)
        value.to_s
      end

      def decode(str)
        str
      end

      def wrap(record, name, value)
        value
      end
      
      def type
        self.class.name.sub(/^DatastaxRails::Types::/,'').sub(/Type$/,'').underscore.to_sym
      end
      
      def full_solr_range
        '[\"\" TO *]'
      end
    end
  end
end