module DatastaxRails
  module Types
    class DynamicMap < ActiveSupport::HashWithIndifferentAccess
      include DirtyCollection
            
      def dup
        self.class.new(record, name, self).tap do |new_hash|
          new_hash.default = default
        end
      end
      
      def [](key)
        super(convert_key(key))
      end
      
      protected
        def convert_key(key)
          unless key.to_s.starts_with?(name)
            key = name + key.to_s
          end
          super(key)
        end
    end
  end
end