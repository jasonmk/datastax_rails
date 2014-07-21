module DatastaxRails
  module AttributeMethods
    class Definition
      attr_reader :klass, :name, :coder, :lazy
      def initialize(klass, name, coder, options)
        @klass  = klass
        @name   = name.to_s
        @lazy   = options.delete(:lazy)
        @coder  = coder.new(options)
      end

      def instantiate(record, value)
        value = coder.default if value.nil?
        return if value.nil?

        value = coder.decode(value)
        coder.wrap(record, name, value)
      end

      # Returns :solr, :cassandra, :both, or +false+
      def indexed
        coder.options[:indexed]
      end

      def type
        coder.type
      end
    end
  end
end
