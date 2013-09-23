module DatastaxRails
  module Associations
    # = DatastaxRails Association Collection
    #
    # CollectionAssociation is an abstract class that provides common stuff to
    # ease the implementation of association proxies that represent
    # collections. See the class hierarchy in AssociationProxy.
    #
    # You need to be careful with assumptions regarding the target: The proxy
    # does not fetch records from the database until it needs them, but new
    # ones created with +build+ are added to the target. So, the target may be
    # non-empty and still lack children waiting to be read from the database.
    # If you look directly to the database you cannot assume that's the entire
    # collection because new records may have been added to the target, etc.
    #
    # If you need to work on all current children, new and existing records,
    # +load_target+ and the +loaded+ flag are your friends.
    class CollectionAssociation < Association #:nodoc:
      attr_reader :proxy
      
      delegate :count, :size, :empty?, :any?, :many?, :first, :last, :to => :scoped
      
      def initialize(owner, reflection)
        super
        @proxy = CollectionProxy.new(self)
      end
      
      # Implements the reader method, e.g. foo.items for Foo.has_many :items
      def reader(force_reload = false)
        if force_reload || stale_target?
          reload
        end
        
        proxy
      end
      
      # Implements the writer method, e.g. foo.items= for Foo.has_many :items
      def writer(records)
        replace(records)
      end
      
      # Implements the ids reader method, e.g. foo.item_ids for Foo.has_many :items
      def ids_reader
        if loaded?
          load_target.map do |record|
            record.send(reflection.association_primary_key)
          end
        else
          scoped.map! do |record|
            record.send(reflection.association_primary_key)
          end
        end
      end
      
      # Implements the ids writer method, e.g. foo.item_ids= for Foo.has_many :items
      def ids_writer(ids)
        ids = Array.wrap(ids).reject { |id| id.blank? }
        replace(klass.find(ids).index_by { |r| r.id }.values_at(*ids))
      end
      
      def reset
        @loaded = false
        @target = []
      end
      
      def build(attributes = {}, options = {}, &block)
        if attributes.is_a?(Array)
          attributes.collect { |attr| build(attr, options, &block) }
        else
          add_to_target(build_record(attributes, options)) do |record|
            yield(record) if block_given?
          end
        end
      end

      def create(attributes = {}, options = {}, &block)
        create_record(attributes, options, &block)
      end

      def create!(attributes = {}, options = {}, &block)
        create_record(attributes, options, true, &block)
      end
      
      # Remove all records from this association
      #
      # See delete for more info.
      def delete_all
        delete(load_target).tap do
          reset
          loaded!
        end
      end

      # Destroy all the records from this association.
      #
      # See destroy for more info.
      def destroy_all
        destroy(load_target).tap do
          reset
          loaded!
        end
      end
      
      # Removes +records+ from this association calling +before_remove+ and
      # +after_remove+ callbacks.
      #
      # This method is abstract in the sense that +delete_records+ has to be
      # provided by descendants. Note this method does not imply the records
      # are actually removed from the database, that depends precisely on
      # +delete_records+. They are in any case removed from the collection.
      def delete(*records)
        delete_or_destroy(records, options[:dependent])
      end

      # Destroy +records+ and remove them from this association calling
      # +before_remove+ and +after_remove+ callbacks.
      #
      # Note that this method will _always_ remove records from the database
      # ignoring the +:dependent+ option.
      def destroy(*records)
        records = find(records) if records.any? { |record| record.kind_of?(Fixnum) || record.kind_of?(String) }
        delete_or_destroy(records, :destroy)
      end
      
      def uniq(collection = load_target)
        seen = {}
        collection.find_all do |record|
          seen[record.id] = true unless seen.key?(record.id)
        end
      end
      
      def load_target
        if find_target?
          @target = merge_target_lists(find_target, target)
        end

        loaded!
        target
      end
      
      def add_to_target(record)
        callback(:before_add, record)
        yield(record) if block_given?

        if options[:uniq] && index = @target.index(record)
          @target[index] = record
        else
          @target << record
        end

        callback(:after_add, record)
        set_inverse_instance(record)

        record
      end
      
      # Replace this collection with +other_array+
      # This will perform a diff and delete/add only records that have changed.
      def replace(other_array)
        other_array.each { |val| raise_on_type_mismatch(val) }
        original_target = load_target.dup

        delete(target - other_array)

        unless concat(other_array - target)
          @target = original_target
          raise RecordNotSaved, "Failed to replace #{reflection.name} because one or more of the " \
                                "new records could not be saved."
        end
      end
      
      # Add +records+ to this association. Returns +self+ so method calls may be chained.
      # Since << flattens its argument list and inserts each record, +push+ and +concat+ behave identically.
      def concat(*records)
        result = true
        load_target if owner.new_record?

        records.flatten.each do |record|
          raise_on_type_mismatch(record)
          add_to_target(record) do |r|
            result &&= insert_record(record) unless owner.new_record?
          end
        end

        result && records
      end
      
      private
      
        # We have some records loaded from the database (persisted) and some that are
        # in-memory (memory). The same record may be represented in the persisted array
        # and in the memory array.
        #
        # So the task of this method is to merge them according to the following rules:
        #
        #   * The final array must not have duplicates
        #   * The order of the persisted array is to be preserved
        #   * Any changes made to attributes on objects in the memory array are to be preserved
        #   * Otherwise, attributes should have the value found in the database
        def merge_target_lists(persisted, memory)
          return persisted if memory.empty?
          return memory if persisted.empty?

          persisted.map! do |record|
            # Unfortunately we cannot simply do memory.delete(record) since on 1.8 this returns
            # record rather than memory.at(memory.index(record)). The behavior is fixed in 1.9.
            mem_index = memory.index(record)

            if mem_index
              mem_record = memory.delete_at(mem_index)

              (record.attribute_names - mem_record.changes.keys).each do |name|
                mem_record[name] = record[name]
              end

              mem_record
            else
              record
            end
          end

          persisted + memory
        end
        
        def find_target
          records = scoped.all
          records = options[:uniq] ? uniq(records) : records
          records.each { |record| set_inverse_instance(record) }
          records
        end
        
        def create_record(attributes, options, raise = false, &block)
          unless owner.persisted?
            raise DatastaxRails::RecordNotSaved, "You cannot call create unless the parent is saved"
          end

          if attributes.is_a?(Array)
            attributes.collect { |attr| create_record(attr, options, raise, &block) }
          else
            add_to_target(build_record(attributes, options)) do |record|
              yield(record) if block_given?
              insert_record(record, true, raise)
            end
          end
        end

        # Do the relevant stuff to insert the given record into the association collection.
        def insert_record(record, validate = true, raise = false)
          raise NotImplementedError
        end
        
        def create_scope
          scoped.scope_for_create.stringify_keys
        end

        def delete_or_destroy(records, method)
          records = records.flatten
          records.each { |record| raise_on_type_mismatch(record) }
          existing_records = records.reject { |r| r.new_record? }

          records.each { |record| callback(:before_remove, record) }

          delete_records(existing_records, method) if existing_records.any?
          records.each { |record| target.delete(record) }

          records.each { |record| callback(:after_remove, record) }
        end
        
        # Delete the given records from the association, using one of the methods :destroy,
        # :delete_all or :nullify (or nil, in which case a default is used).
        def delete_records(records, method)
          raise NotImplementedError
        end

        def callback(method, record)
          callbacks_for(method).each do |callback|
            case callback
            when Symbol
              owner.send(callback, record)
            when Proc
              callback.call(owner, record)
            else
              callback.send(method, owner, record)
            end
          end
        end

        def callbacks_for(callback_name)
          full_callback_name = "#{callback_name}_for_#{reflection.name}"
          owner.class.send(full_callback_name.to_sym) || []
        end
        
        def include_in_memory?(record)
          if reflection.is_a?(DatastaxRails::Reflection::ThroughReflection)
            owner.send(reflection.through_reflection.name).any? { |source|
              target = source.send(reflection.source_reflection.name)
              target.respond_to?(:include?) ? target.include?(record) : target == record
            } || target.include?(record)
          else
            target.include?(record)
          end
        end
    end
  end
end