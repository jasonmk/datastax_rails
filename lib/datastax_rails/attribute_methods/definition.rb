module DatastaxRails
  module AttributeMethods
    class Definition
      attr_reader :name, :coder, :lazy
      def initialize(name, coder, options)
        @name   = name.to_s
        @lazy   = options.delete(:lazy)
        @coder  = coder.new(options)
      end

      def instantiate(record, value)
        value ||= coder.default
        return unless value
      
        value = value.kind_of?(String) ? coder.decode(value) : value
        coder.wrap(record, name, value)
      end
    end
  end
end