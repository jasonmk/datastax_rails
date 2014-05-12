module DatastaxRails
  module Types
    class DynamicList < Array
      include DirtyCollection

      def initialize(record, name, collection)
        super(record, name, collection || [])
      end
    end
  end
end