module DatastaxRails
  module Types
    # A collection type that allows you to store ordered arrays in Cassandra.
    # Changes are tracked by hooking into ActiveModel's built-in change
    # tracking.
    class DynamicList < Array
      include DirtyCollection

      def initialize(record, name, collection)
        super(record, name, collection || [])
      end
    end
  end
end
