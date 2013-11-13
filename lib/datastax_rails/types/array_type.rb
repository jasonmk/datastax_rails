module DatastaxRails
  module Types
    # ArrayType is used for storing arrays in Datastax Enterprise.
    # They are indexed into SOLR as discrete values so that you can do something like this:
    #
    #   Post.where(:tags => 'Technology')
    #
    # That would give you all the posts that have Technology somewhere in the tags array. 
    class ArrayType < BaseType
      DEFAULTS = {:solr_type => 'array', :indexed => :solr, :stored => true, :multi_valued => false, :sortable => false, :tokenized => true, :fulltext => true, :cassandra_type => 'list'}
      
      # An extension to normal arrays that allow for tracking of dirty values.  This is
      # used by ActiveModel's change tracking framework.
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
        ar.join("$$$$")
      end

      def decode(str)
        return [] if str.blank?
        #                                         Temporary fix
        str.is_a?(Array) ? str.flatten : str.gsub(/&&&&/,'$$$$').split(/\$\$\$\$/).reject{|a|a.blank?} 
      end

      def wrap(record, name, value)
        DirtyArray.new(record, name, value, options)
      end
    end
  end
end