module DatastaxRails
  module AttributeMethods
    module Typecasting
      extend ActiveSupport::Concern

      included do
        class_attribute :attribute_definitions
        class_attribute :lazy_attributes
        class_attribute :readonly_attributes
        self.attribute_definitions = {}
        self.lazy_attributes = []
        self.readonly_attributes = []

      end

      module ClassMethods
        # Provide some measure of compatibility with things that expect this from ActiveRecord.
        def columns_hash
          self.attribute_defintions
        end 
        
        # We need to ensure that inherited classes get their own attribute definitions.
        # In addition, we take the opportunity to track all the DatastaxRails::Base decendents.
        # This will be useful when it comes to things like schema generation.
        def inherited(child)
          super
          child.attribute_definitions = attribute_definitions.dup
          self.models << child
        end
        
        # @!group Attribute Types
        
        # @!macro [new] attr_doc
        #   Declare an attribute of the given type
        #   
        #   @param [Symbol] name the name of the attribute to create
        #   @param [Hash] options the options to use in setting up the attribute
        def binary(name, options = {})
          options.reverse_merge!(:lazy => true)
          attribute(name, options.update(:type => :binary))
        end
        
        # Declare the timestamps attribute type method.
        # Creates both the created_at and updated_at attributes with type +time+.
        # 
        # @param [Hash] options the options to use in setting up the attribute
        def timestamps(options = {})
          attribute(:created_at, options.update(:type => :time))
          attribute(:updated_at, options.update(:type => :time))
        end
        
        # @!method array(name, options = {})
        #   @macro attr_doc
        # @!method boolean(name, options = {})
        #   @macro attr_doc
        # @!method date(name, options = {})
        #   @macro attr_doc
        # @!method float(name, options = {})
        #   @macro attr_doc
        # @!method integer(name, options = {})
        #   @macro attr_doc
        # @!method json(name, options = {})
        #   @macro attr_doc
        # @!method string(name, options = {})
        #   @macro attr_doc
        # @!method text(name, options = {})
        #   @macro attr_doc
        # @!method time(name, options = {})
        #   @macro attr_doc
        # @!method time_with_zone(name, options = {})
        #   @macro attr_doc
        
        # The following sets up a bunch of nearly identical attribute methods
        %w(array boolean date float integer json string text time time_with_zone).each do |type|
          class_eval <<-EOV, __FILE__, __LINE__ + 1
            def #{type}(name, options = {})                               # def string(name, options = {})
              attribute(name, options.update(:type => :#{type}))             #   attribute(name, options.update(type: :string))
            end                                                           # end
          EOV
        end
        
        # @!endgroup

        # Casts a single attribute according to the appropriate coder.
        #
        # @param [DatastaxRails::Base] record the record to which this attribute belongs
        # @param [String] name the name of the attribute
        # @param [Object] value the value of the attribute prior to typecasting
        #
        # @return the typecast value
        # @raise [NoMethodError] if the attribute is unknown
        def typecast_attribute(record, name, value)
          if attribute_definition = attribute_definitions[name.to_sym]
            attribute_definition.instantiate(record, value)
          else
            raise NoMethodError, "Unknown attribute #{name.inspect}"
          end
        end
      end
    end
  end
end