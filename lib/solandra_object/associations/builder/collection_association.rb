module DatastaxRails::Associations::Builder #:nodoc:
  class CollectionAssociation < Association #:nodoc:
    CALLBACKS = [:before_add, :after_add, :before_remove, :after_remove]
    
    self.valid_options += [:columm_family, :order, :uniq, :before_add, :before_remove, :after_add, :after_remove]
    
    def self.build(model, name, options)
      new(model, name, options).build
    end
    
    def build
      reflection = super
      CALLBACKS.each { |callback_name| define_callback(callback_name) }
    end
    
    def writable?
      true
    end
    
    protected
    
      def define_callback(callback_name)
        full_callback_name = "#{callback_name}_for_#{name}"
        
        # XXX : why do i need method_defined? I think its because of the inheritance chain
        model.class_attribute full_callback_name.to_sym unless model.method_defined?(full_callback_name)
        model.send("#{full_callback_name}=", Array.wrap(options[callback_name.to_sym]))
      end
      
      def define_readers
        super
        
        name = self.name
        mixin.redefine_method("#{name.to_s.singularize}_ids") do
          association(name).ids_reader
        end
      end
      
      def define_writers
        super
        
        name = self.name
        mixin.redefine_method("#{name.to_s.singularize}_ids=") do |ids|
          association(name).ids_writer(ids)
        end
      end
  end
end