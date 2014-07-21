module DatastaxRails
  module Types
    # An extension to normal arrays and hashes that allow for tracking of dirty values.  This is
    # used by ActiveModel's change tracking framework.
    module DirtyCollection
      extend ActiveSupport::Concern
      cattr_accessor :dsr_ignore_modifications

      included do
        attr_accessor :record, :name

        ms = [:<<, :delete, :[]=, :add, :subtract, :store, :push, :pop, :unshift, :shift, :insert, :clear] +
             ActiveSupport::HashWithIndifferentAccess.instance_methods(true).select { |m| m.to_s.ends_with?('!') } +
             Array.instance_methods(true).select { |m| m.to_s.ends_with?('!') } +
             Set.instance_methods(true).select { |m| m.to_s.ends_with?('!') }

        ms.each do |m|
          next unless instance_methods.include?(m)
          alias_method "___#{m}", m
          original_method = instance_method(m)
          define_method(m) do |*args, &block|
            modifying do
              original_method.bind(self).call(*args, &block)
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

      # FIXME: How do we make this thread-safe?
      def self.ignore_modifications
        original = dsr_ignore_modifications
        self.dsr_ignore_modifications = true
        result = yield
        self.dsr_ignore_modifications = original
        result
      end

      private

      def modifying
        # So there's a problem with overriding the map! method on Array.
        # When we do the update to record.attributes, HashWithIndifferentAccess
        # calls .map! on our Array.  This causes infinite recursion which
        # I find is generally not a desired behavior.  We use a variable
        # to tell if we've already hijacked the call.
        if dsr_ignore_modifications
          yield
        else
          DirtyCollection.ignore_modifications do
            original = dup unless record.changed_attributes.key?(name)

            result = yield

            organize_collection

            if !record.changed_attributes.key?(name) && original != self
              record.changed_attributes[name] = original
            end

            record.attributes[name] = self

            result
          end
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
