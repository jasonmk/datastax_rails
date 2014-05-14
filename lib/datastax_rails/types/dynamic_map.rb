module DatastaxRails
  module Types
    # A collection type that allows you to store key/value pairs in Cassandra.
    # Changes are tracked by hooking into ActiveModel's built-in change
    # tracking.
    #
    # Keys are converted to have the name of the collection prefixed
    # to them as this is how the Solr/Cassandra integration converts
    # between them and dynamic fields.
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