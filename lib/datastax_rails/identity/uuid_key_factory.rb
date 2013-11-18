module DatastaxRails
  module Identity
    # Key factories need to support 3 operations
    class UUIDKeyFactory < AbstractKeyFactory
      class UUID < SimpleUUID::UUID
        def to_s
          to_guid
        end
      end
      
      def initialize(options = {})
        @key_columns = options[:column].blank? ? ['key'] : Array.wrap(options[:column])
      end
    
      def next_key(object)
        UUID.new
      end
      
      def parse(string)
        UUID.new(string) if string
      rescue
        nil
      end
    end
  end
end

