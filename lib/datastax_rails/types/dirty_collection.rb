# An extension to normal arrays and hashes that allow for tracking of dirty values.  This is
# used by ActiveModel's change tracking framework.
module DatastaxRails
  module Types
    module DirtyCollection
      extend ActiveSupport::Concern
      
      included do
        attr_accessor :record, :name
        
        methods = [:<<, :delete, :[]=, :add, :subtract, :store, :push, :pop, :unshift, :shift, :insert, :clear] +
                   ActiveSupport::HashWithIndifferentAccess.instance_methods(true).select{|m| m.to_s.ends_with?('!')} +
                   Array.instance_methods(true).select{|m| m.to_s.ends_with?('!')} +
                   Set.instance_methods(true).select{|m| m.to_s.ends_with?('!')}
                
        methods.each do |m|
          if self.instance_methods.include?(m)
            original_method = self.instance_method(m)
            define_method(m) do |*args, &block|
              modifying do
                original_method.bind(self).call(*args, &block)
              end
            end
          end
        end
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
          # So there's a problem with overriding the map! method on Array.
          # When we do the update to record.attributes, HashWithIndifferentAccess
          # calls .map! on our Array.  This causes infinite recursion which
          # I find is generally not a desired behavior.  We use a variable
          # to tell if we've already hijacked the call.
          if @hijacked
            yield
          else
            @hijacked = true
            unless record.changed_attributes.key?(name)
              original = dup
            end
  
            result = yield
            
            organize_collection
  
            if !record.changed_attributes.key?(name) && original != self
              record.changed_attributes[name] = original
            end
            
            record.attributes[name] = self
            
            @hijacked = false
            result
          end
        end
        
        # A hook to allow implementing classes to muck with the collection
        # before we check it for equality.
        def organize_collection
          # No-op
        end
    end
  end
end