module DatastaxRails
  module AttributeMethods
    # Handles the mapping of attributes to their appropriate DatastaxRails::Column so that they can
    # be typecasted.
    module Typecasting
      extend ActiveSupport::Concern

      included do
        class_attribute :attribute_definitions
        class_attribute :lazy_attributes
        class_attribute :readonly_attributes
        self.attribute_definitions = ActiveSupport::HashWithIndifferentAccess.new
        self.lazy_attributes = []
        self.readonly_attributes = []
      end

      # This is a hook for use by modules that need to do extra stuff to
      # attributes when they are initialized. (e.g. attribute
      # serialization)
      def initialize_attributes(attributes) #:nodoc:
        attrs = {}
        attributes.each do |k, v|
          col = column_for_attribute(k)
          next unless col
          if col.type == :map && k.to_s != col.name.to_s
            # See if we have a matching dynamic attribute column
            self.class.map_columns.each do |mcol|
              if k.to_s.starts_with?(mcol.name.to_s)
                attrs[mcol.name.to_s] ||= mcol.wrap_collection({}, self)
                attrs[mcol.name.to_s][k.to_s] = v
              end
            end
          else
            attrs[k.to_s] = col.collection? ? col.wrap_collection(v, self) : v
          end
        end
        attrs
      end

      module ClassMethods
        # Provide some measure of compatibility with things that expect this from ActiveRecord.
        def columns_hash
          attribute_definitions
        end

        # Gives you all of the map columns (useful for detecting dynamic columns)
        def map_columns
          @map_columns ||= attribute_definitions.values.select { |c| c.type == :map }
        end

        # Returns a hash where the keys are column names and the values are
        # default values when instantiating the DSR object for this table.
        def column_defaults
          @column_defaults ||= Hash[columns.map { |c| [c.name.to_s, c.default] }]
        end

        # We need to ensure that inherited classes get their own attribute definitions.
        def inherited(child)
          super
          child.attribute_definitions = attribute_definitions.dup
        end

        # @!group Attribute Types

        # @!macro [new] attr_doc
        #   Declare an attribute of the given type
        #
        #   @param [Symbol] name the name of the attribute to create
        #   @param [Hash] options the options to use in setting up the attribute
        def binary(name, options = {})
          options.reverse_merge!(lazy: true)
          attribute(name, options.update(type: :binary))
        end

        # Declare the timestamps attribute type method.
        # Creates both the created_at and updated_at attributes with type +time+.
        #
        # @param [Hash] options the options to use in setting up the attribute
        def timestamps(options = {})
          attribute(:created_at, options.update(type: :timestamp))
          attribute(:updated_at, options.update(type: :timestamp))
        end

        # @!method array(name, options = {})
        #   @macro attr_doc
        # @!method boolean(name, options = {})
        #   @macro attr_doc
        # @!method date(name, options = {})
        #   @macro attr_doc
        # @!method datetime(name, options = {})
        #   @macro attr_doc
        # @!method double(name, options = {})
        #   @macro attr_doc
        # @!method float(name, options = {})
        #   @macro attr_doc
        # @!method integer(name, options = {})
        #   @macro attr_doc
        # @!method long(name, options = {})
        #   @macro attr_doc
        # @!method string(name, options = {})
        #   @macro attr_doc
        # @!method text(name, options = {})
        #   @macro attr_doc
        # @!method time(name, options = {})
        #   @macro attr_doc
        # @!method timestamp(name, options = {})
        #   @macro attr_doc
        # @!method uuid(name, options = {})
        #   @macro attr_doc
        # @!method map(name, options = {})
        #   @macro attr_doc
        # @!method list(name, options = {})
        #   @macro attr_doc
        # @!method set(name, options = {})
        #   @macro attr_doc

        # The following sets up a bunch of nearly identical attribute methods
        %w(array boolean date datetime double float integer long string text time timestamp
           uuid map set list).each do |type|
          class_eval <<-EOV, __FILE__, __LINE__ + 1
            def #{type}(name, options = {})                        # def string(name, options = {})
              attribute(name, options.update(:type => :#{type}))   #   attribute(name, options.update(type: :string))
            end                                                    # end
          EOV
        end

        # @!endgroup
      end
    end
  end
end
