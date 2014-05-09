# An extension to normal arrays and hashes that allow for tracking of dirty values.  This is
# used by ActiveModel's change tracking framework.
module DatastaxRails
  module Types
    module DirtyCollection
      extend ActiveSupport::Concern
      
      included do
        attr_accessor :record, :name
      end
      
      def initialize(record, name, collection)
        @record   = record
        @name     = name.to_s

        super(collection)
        
        organize_collection
      end
      
      def delete(obj)
        modifying do
          super
        end
      end
      
      private
        def modifying
          unless record.changed_attributes.key?(name)
            original = dup
          end

          result = yield
          
          organize_collection

          if !record.changed_attributes.key?(name) && original != self
            record.changed_attributes[name] = original
          end
          record.attributes[name] = self

          result
        end
        
        # A hook to allow implementing classes to muck with the collection
        # before we check it for equality.
        def organize_collection
          # No-op
        end
    end
  end
end