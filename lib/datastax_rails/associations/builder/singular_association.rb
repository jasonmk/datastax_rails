module DatastaxRails::Associations::Builder
  class SingularAssociation < Association #:nodoc:
    #self.valid_options += [:remote, :dependent, :counter_cache, :primary_key, :inverse_of]
    self.valid_options += [:dependent, :denorm]
    
    def constructable?
      true
    end

    def define_accessors
      super
      define_constructors if constructable?
    end
    
    def build
      relation = super
      configure_denorm
      relation
    end

    private
    
      def configure_denorm
        if options[:denorm]
          unless options[:denorm].is_a?(Hash)
            raise ArgumentError, "The :denorm option expects a hash in the form {:attr_on_other_model => :virtual_attr_on_this_model}"
          end
    
          method_name = "belongs_to_dependent_#{options[:dependent]}_for_#{name}"
          model.send(:class_eval, <<-eoruby, __FILE__, __LINE__ + 1)
            def #{method_name}
              association = #{name}
              association.#{options[:dependent]} if association
            end
            eoruby
          model.after_destroy method_name
        end
      end

      def define_constructors
        name = self.name

        model.redefine_method("build_#{name}") do |*params, &block|
          association(name).build(*params, &block)
        end

        model.redefine_method("create_#{name}") do |*params, &block|
          association(name).create(*params, &block)
        end

        model.redefine_method("create_#{name}!") do |*params, &block|
          association(name).create!(*params, &block)
        end
      end
  end
end