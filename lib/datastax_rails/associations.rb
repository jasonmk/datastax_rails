# rubocop:disable Style/PredicateName
module DatastaxRails
  # Associations holds all the functionality related to DSR associations
  module Associations
    extend ActiveSupport::Concern
    extend ActiveSupport::Autoload

    autoload :Association
    autoload :AssociationScope
    autoload :SingularAssociation
    autoload :CollectionAssociation
    autoload :CollectionProxy
    autoload :BelongsToAssociation
    autoload :HasOneAssociation
    autoload :HasManyAssociation

    # Builder holds all the classes related to building the association
    module Builder
      extend ActiveSupport::Autoload

      autoload :Association
      autoload :SingularAssociation
      autoload :CollectionAssociation

      autoload :BelongsTo
      autoload :HasOne
      autoload :HasMany
    end

    # Clears out the association cache.
    def clear_association_cache #:nodoc:
      @association_cache.clear if persisted?
    end

    # :nodoc:
    attr_reader :association_cache

    # Returns the association instance for the given name, instantiating it if it doesn't already exist
    def association(name) #:nodoc:
      association = association_instance_get(name)

      if association.nil?
        reflection = self.class.reflect_on_association(name)
        association = reflection.association_class.new(self, reflection)
        association_instance_set(name, association)
      end

      association
    end

    private

    # Returns the specified association instance if it responds to :loaded?, nil otherwise.
    def association_instance_get(name)
      @association_cache ||= {}
      @association_cache[name.to_sym]
    end

    # Set the specified association instance.
    def association_instance_set(name, association)
      @association_cache ||= {}
      @association_cache[name] = association
    end

    module ClassMethods
      def belongs_to(name, options = {})
        Builder::BelongsTo.build(self, name, options)
      end

      def has_many(name, options = {})
        Builder::HasMany.build(self, name, options)
        # klass = options[:class_name]
        # klass ||= name.to_s.singularize
        # foreign_key = options[:foreign_key]
        # foreign_key ||= self.name.foreign_key
        # define_method name do
        # klass.where(foreign_key)
        # end
      end

      def has_and_belongs_to_many(_name, _options = {})
      end

      def has_one(name, options = {})
        Builder::HasOne.build(self, name, options)
      end
    end
  end
end
