module DatastaxRails::Associations::Builder  # rubocop:disable Style/ClassAndModuleChildren
  class SingularAssociation < Association #:nodoc:
    # self.valid_options += [:remote, :dependent, :counter_cache, :primary_key, :inverse_of]
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
      return unless options[:denorm]
      options[:denorm].is_a?(Hash) || fail(ArgumentError, 'The :denorm option expects a hash in the form ' \
                                                          '{:attr_on_other_model => :virtual_attr_on_this_model}')

      # options[:denorm].each do |remote, local|
      # # Default everything to a string.  If it should be something different,
      # #   the developer can declare the attribute manually.
      # model.send(:string, local)
      # model.send(:class_eval, <<-eoruby, __FILE__, __LINE__ + 1)
      # def #{local}
      # eoruby
      # end
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
