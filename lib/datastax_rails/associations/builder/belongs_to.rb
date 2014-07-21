module DatastaxRails::Associations::Builder # rubocop:disable Style/ClassAndModuleChildren
  class BelongsTo < SingularAssociation
    self.macro = :belongs_to

    def build
      reflection = super
      configure_dependency
      reflection
    end

    private

    def configure_dependency
      return unless options[:dependent]
      unless options[:dependent].in?([:destroy, :delete])
        fail ArgumentError, "The :dependent option expects either :destroy or :delete (#{options[:dependent].inspect})"
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
end
