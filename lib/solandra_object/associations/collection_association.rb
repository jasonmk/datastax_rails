module DatastaxRails
  module Associations
    class CollectionAssociation < Association #:nodoc:
      attr_reader :proxy
      
      delegate :find, :first, :all, :count, :size, :delete, :destroy, :delete_all, :destroy_all, :to => :scoped 
      delegate :empty?, :any?, :many?, :loaded?, :to => :scoped
      
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
      
      def load_target
        @target = find_target
        loaded!
        target
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
          records.each { |record| set_inverse_instance(record) }
        end
    end
  end
end