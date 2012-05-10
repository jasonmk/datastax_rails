module DatastaxRails
  module Types
    class JsonType < BaseType
      def encode(hash)
        ActiveSupport::JSON.encode(hash)
      end

      def decode(str)
        ActiveSupport::JSON.decode(str)
      end
    end
  end
end