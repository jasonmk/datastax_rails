module DatastaxRails
  module Types
    class ArrayType < BaseType
      DEFAULTS = {:solr_type => 'array', :indexed => true, :stored => true, :multi_valued => false, :sortable => false, :tokenized => true, :fulltext => false}
      class DirtyArray < Array
        attr_accessor :record, :name, :options
        def initialize(record, name, array, options)
          @record   = record
          @name     = name.to_s
          @options  = options

          super(array)
          setify!
        end

        def <<(obj)
          modifying do
            super
            setify!
          end
        end

        def delete(obj)
          modifying do
            super
          end
        end

        private
          def setify!
            if options[:unique]
              compact!
              uniq!
              begin sort! rescue ArgumentError end
            end
          end

          def modifying
            unless record.changed_attributes.include?(name)
              original = dup
            end

            result = yield

            if !record.changed_attributes.key?(name) && original != self
              record.changed_attributes[name] = original
            end

            record.send("#{name}=", self)

            result
          end
      end

      def default
        []
      end

      def encode(array)
        raise ArgumentError.new("#{self} requires an Array") unless array.kind_of?(Array)
        ar = Array(array)
        ar.uniq! if options[:unique]
        ar.join("&&&&")
      end

      def decode(str)
        return [] if str.blank?
        
        str.split(/&&&&/)
      end

      def wrap(record, name, value)
        DirtyArray.new(record, name, value, options)
      end
    end
  end
end