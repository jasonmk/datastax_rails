require 'mutex_m'

module DatastaxRails
  module AttributeMethods
    extend ActiveSupport::Concern
    include ActiveModel::AttributeMethods

    included do
      initialize_generated_modules

      include Write
      include Dirty
      include Read
      include PrimaryKey
      include Typecasting

      alias_method :[], :read_attribute
      alias_method :[]=, :write_attribute
      alias_method :has_attribute?, :attribute_exists?
    end

    module ClassMethods
      def inherited(child_class)
        child_class.initialize_generated_modules
        super
      end

      def initialize_generated_modules
        @generated_attribute_methods = Module.new do
          extend Mutex_m

          const_set :AttrNames, Module.new {
            def self.set_name_cache(name, value)
              const_name = "ATTR_#{name}"
              unless const_defined? const_name
                const_set const_name, value.dup.freeze
              end
            end
          }
        end
        @attribute_methods_generated = false
        include @generated_attribute_methods
      end

      def define_attribute_methods
        # Use a mutex; we don't want two thread simultaneously trying to define
        # attribute methods.
        generated_attribute_methods.synchronize do
          return false if attribute_methods_generated?
          super(attribute_definitions.keys)
          # Remove setter methods from readonly attributes
          readonly_attributes.each do |attr|
            remove_method("#{attr}=".to_sym) if method_defined?("#{attr}=".to_sym)
          end
          @attribute_methods_generated = true
        end
        true
      end

      def undefine_attribute_methods
        generated_attribute_methods.synchronize do
          super if attribute_methods_generated?
          @attribute_methods_generated = false
        end
      end

      def attribute_methods_generated?
        @attribute_methods_generated ||= false
      end

      #
      # attribute :name, :type => :string
      # attribute :ammo, :type => Ammo, :coder => AmmoCodec
      #
      def attribute(name, options)
        type  = options.delete :type
        coder = options.delete :coder
        default = options.delete :default

        lazy_attributes << name.to_sym if options[:lazy]
        readonly_attributes << name.to_sym if options[:readonly]

        column = Column.new(name, default, type, options)
        column.primary = (name.to_s == primary_key.to_s)
        if coder
          coder = coder.constantize rescue nil # rubocop:disable Style/RescueModifier
          if coder.class == Class && (coder.instance_methods & [:dump, :load]).size == 2
            column.coder = coder.new(self)
          else
            fail ArgumentError, 'Coder must be a class that responds the dump and load instance variables'
          end
        end
        attribute_definitions[name.to_sym] = column
      end

      # Returns the column object for the named attribute. Returns +nil+ if the
      # named attribute not exists.
      #
      #   class Person < DatastaxRails::Base
      #   end
      #
      #   person = Person.new
      #   person.column_for_attribute(:name)
      #   # => #<DatastaxRails::Base:0x007ff4ab083980 @name="name", @sql_type="varchar(255)", @null=true, ...>
      #
      # person.column_for_attribute(:nothing)
      # # => nil
      def column_for_attribute(name)
        # FIXME: should this return a null object for columns that don't exist?
        column = columns_hash[name.to_s]
        unless column
          # Check for a dynamic column
          columns_hash.values.each do |col|
            next unless col.type == :map && name.to_s.starts_with?("#{col.name}")
            column = col
            break
          end
        end
        column
      end
    end

    def attribute_exists?(name)
      @attributes.key?(name.to_s)
    end

    def method_missing(method_id, *args, &block)
      if !self.class.attribute_methods_generated?
        self.class.define_attribute_methods
        send(method_id, *args, &block)
      else
        super
      end
    end

    def respond_to?(*args)
      self.class.define_attribute_methods unless self.class.attribute_methods_generated?
      super
    end

    def column_for_attribute(name)
      self.class.column_for_attribute(name)
    end

    protected

    def attribute_method?(name)
      attribute_definitions.key?(name.to_sym)
    end

    def clone_attributes(reader_method = :read_attribute, attributes = {}) # :nodoc:
      attribute_names.each do |name|
        attributes[name] = clone_attribute_value(reader_method, name)
      end
      attributes
    end

    def clone_attribute_value(reader_method, attribute_name) # :nodoc:
      value = send(reader_method, attribute_name)
      value.duplicable? ? value.clone : value
    rescue TypeError, NoMethodError
      value
    end

    private

    def attribute(name)
      read_attribute(name)
    end

    def attribute=(name, value)
      write_attribute(name, value)
    end
  end
end
