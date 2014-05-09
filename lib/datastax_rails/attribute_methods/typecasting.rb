module DatastaxRails
  module AttributeMethods
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

      module ClassMethods
        # Provide some measure of compatibility with things that expect this from ActiveRecord.
        def columns_hash
          self.attribute_definitions
        end
        
        # This is a hook for use by modules that need to do extra stuff to
        # attributes when they are initialized. (e.g. attribute
        # serialization)
        def initialize_attributes(attributes, options = {}) #:nodoc:
          attributes
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
          options.reverse_merge!(:lazy => true)
          attribute(name, options.update(:type => :binary))
        end
        
        # Declare the timestamps attribute type method.
        # Creates both the created_at and updated_at attributes with type +time+.
        # 
        # @param [Hash] options the options to use in setting up the attribute
        def timestamps(options = {})
          attribute(:created_at, options.update(:type => :timestamp))
          attribute(:updated_at, options.update(:type => :timestamp))
        end
        
        # @!method array(name, options = {})
        #   @macro attr_doc
        # @!method boolean(name, options = {})
        #   @macro attr_doc
        # @!method date(name, options = {})
        #   @macro attr_doc
        # @!method datetime(name, options = {})
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
        # @!method timestamp(name, options = {})
        #   @macro attr_doc
        # @!method time_with_zone(name, options = {})
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
        %w(array boolean date datetime float integer json string text time timestamp time_with_zone uuid map set list).each do |type|
          class_eval <<-EOV, __FILE__, __LINE__ + 1
            def #{type}(name, options = {})                               # def string(name, options = {})
              attribute(name, options.update(:type => :#{type}))             #   attribute(name, options.update(type: :string))
            end                                                           # end
          EOV
        end
        
        # @!endgroup
      end
    end
  end
end
