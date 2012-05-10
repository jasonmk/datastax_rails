module DatastaxRails::Associations::Builder
  class HasMany < CollectionAssociation #:nodoc:
    self.macro = :has_many
    
    self.valid_options += [:primary_key, :dependent, :source_type]
    
    def build
      reflection = super
      configure_dependency
      reflection
    end
    
    private
      
      def configure_dependency
        if options[:dependent]
          unless options[:dependent].in?([:destroy])  # Only destroy and restrict supported for now
            raise ArgumentError, "The :dependent option only handles :destroy or :restrict for now (#{options[:dependent].inspect})"
          end

          send("define_#{options[:dependent]}_dependency_method")
          model.before_destroy dependency_method_name
        end
      end
      
      def define_destroy_dependency_method
        name = self.name
        mixin.redefine_method(dependency_method_name) do
          send(name).each do |o|
            # No point in executing the counter update since we're going to destroy the parent anyway
            counter_method = ('belongs_to_counter_cache_before_destroy_for_' + self.class.name.downcase).to_sym
            if o.respond_to?(counter_method)
              class << o
                self
              end.send(:define_method, counter_method, Proc.new {})
            end
          end

          send(name).destroy_all
        end
      end
      
      def define_restrict_dependency_method
        name = self.name
        mixin.redefine_method(dependency_method_name) do
          raise DatastaxRails::DeleteRestrictionError.new(name) unless send(name).empty?
        end
      end
      
      def dependency_method_name
        "has_many_dependent_for_#{name}"
      end
  end
end