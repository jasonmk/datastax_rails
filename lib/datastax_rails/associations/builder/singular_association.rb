module DatastaxRails::Associations::Builder
  class SingularAssociation < Association #:nodoc:
    #self.valid_options += [:remote, :dependent, :counter_cache, :primary_key, :inverse_of]
    self.valid_options += [:dependent]
    
    def constructable?
      true
    end

    def define_accessors
      super
      define_constructors if constructable?
    end

    private

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