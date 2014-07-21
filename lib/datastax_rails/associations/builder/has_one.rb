module DatastaxRails::Associations::Builder # rubocop:disable Style/ClassAndModuleChildren
  class HasOne < SingularAssociation #:nodoc:
    self.macro = :has_one

    def build
      reflection = super
      configure_dependency
      reflection
    end

    private

    def validate_options
      valid_options = self.class.valid_options
      valid_options += self.class.through_options if options[:through]
      options.assert_valid_keys(valid_options)
    end

    def configure_dependency
      return unless options[:dependent]
      unless options[:dependent].in?([:destroy, :delete, :nullify, :restrict])
        fail ArgumentError, 'The :dependent option expects either :destroy, :delete, ' \
                             ":nullify or :restrict (#{options[:dependent].inspect})"
      end

      send("define_#{options[:dependent]}_dependency_method")
      model.before_destroy dependency_method_name
    end

    def dependency_method_name
      "has_one_dependent_#{options[:dependent]}_for_#{name}"
    end

    def define_destroy_dependency_method
      model.send(:class_eval, <<-eoruby, __FILE__, __LINE__ + 1)
          def #{dependency_method_name}
            association(#{name.to_sym.inspect}).delete
          end
        eoruby
    end
    alias_method :define_delete_dependency_method, :define_destroy_dependency_method
    alias_method :define_nullify_dependency_method, :define_destroy_dependency_method

    def define_restrict_dependency_method
      name = self.name
      model.redefine_method(dependency_method_name) do
        fail DatastaxRails::DeleteRestrictionError.new(name) unless send(name).nil?
      end
    end
  end
end
