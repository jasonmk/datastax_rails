module DatastaxRails
  module Associations
    class SingularAssociation < Association #:nodoc:
      def reader(force_reload = false)
        reload if force_reload || !loaded? || stale_target?
        target
      end
      
      def writer(record)
        replace(record)
      end
      
      def create(attributes = {}, options = {}, &block)
        create_record(attributes, options, &block)
      end
      
      def create!(attributes = {}, options = {}, &block)
        create_record(attributes, options, true, &block)
      end

      def build(attributes = {}, options = {})
        record = build_record(attributes, options)
        yield(record) if block_given?
        set_new_record(record)
        record
      end
      
      private

        # Implemented by subclasses
        def replace(record)
          raise NotImplementedError, "Subclasses must implement a replace(record) method"
        end

        def set_new_record(record)
          replace(record)
        end

        def create_record(attributes, options, raise_error = false)
          record = build_record(attributes, options)
          yield(record) if block_given?
          saved = record.save
          set_new_record(record)
          raise RecordInvalid.new(record) if !saved && raise_error
          record
        end
    end
  end
end