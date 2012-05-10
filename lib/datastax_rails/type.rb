module DatastaxRails
  class Type
    cattr_accessor :attribute_types
    self.attribute_types = {}.with_indifferent_access

    class << self
      def register(name, coder)
        attribute_types[name] = coder
      end

      def get_coder(name)
        attribute_types[name]
      end
    end
  end
end
