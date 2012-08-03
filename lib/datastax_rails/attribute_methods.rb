module DatastaxRails
  module AttributeMethods
    extend ActiveSupport::Concern
    include ActiveModel::AttributeMethods
    
    included do
      alias :[] :read_attribute
      alias :[]= :write_attribute

      attribute_method_suffix("=")
    end
    
    module ClassMethods
      def define_attribute_methods
        return if attribute_methods_generated?
        super(attribute_definitions.keys)
        # Remove setter methods from readonly attributes
        readonly_attributes.each do |attr|
          remove_method("#{attr}=".to_sym) if method_defined?("#{attr}=".to_sym)
        end
        @attribute_methods_generated = true
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

        if type.is_a?(Symbol)
          coder = DatastaxRails::Type.get_coder(type) || (raise "Unknown type #{type}")
        elsif coder.nil?
          raise "Must supply a :coder for #{name}"
        end
        
        if(options[:lazy])
          lazy_attributes << name.to_sym
        end
        
        if(options[:readonly])
          readonly_attributes << name.to_sym
        end

        attribute_definitions[name.to_sym] = AttributeMethods::Definition.new(name, coder, options)
      end
    end
    
    def write_attribute(name, value)
      @attributes[name.to_s] = self.class.typecast_attribute(self, name, value)
      loaded_attributes[name] = true
    end

    def read_attribute(name)
      if(!loaded_attributes[name] && persisted?)
        @attributes[name.to_s] = self.class.select(name).with_cassandra.find(self.id).read_attribute(name)
        loaded_attributes[name] = true
      end
        
      @attributes[name.to_s]
    end

    def attribute_exists?(name)
      @attributes.key?(name.to_s)
    end

    def attributes=(attributes)
      attributes.each do |(name, value)|
        send("#{name}=", value)
      end
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

    protected
      def attribute_method?(name)
        !!attribute_definitions[name.to_sym]
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