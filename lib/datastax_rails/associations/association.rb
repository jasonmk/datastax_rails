require 'active_support/core_ext/array/wrap'
require 'active_support/core_ext/object/inclusion'

module DatastaxRails
  module Associations
    # = DatastaxRails Associations
    #
    # This is the root class of all associations ('+ Foo' signifies an included module Foo):
    #
    #   Association
    #     SingularAssociation
    #       HasOneAssociation
    #         HasOneThroughAssociation + ThroughAssociation (Not implemented)
    #       BelongsToAssociation
    #         BelongsToPolymorphicAssociation (Not implemented)
    #     CollectionAssociation
    #       HasAndBelongsToManyAssociation (Not implemented)
    #       HasManyAssociation
    #         HasManyThroughAssociation + ThroughAssociation (Not implemented)
    class Association #:nodoc:
      attr_reader :owner, :target, :reflection, :loaded
      alias_method :loaded?, :loaded

      delegate :options, to: :reflection

      def initialize(owner, reflection)
        reflection.check_validity!

        @target = nil
        @owner, @reflection = owner, reflection
        @updated = false

        reset
        reset_scope
      end

      # Returns the name of the column family name of the related class:
      #
      #   post.comments.aliased_column_family # => "comments"
      #
      def aliased_column_family
        reflection.klass.column_family
      end

      # Resets the \loaded flag to +false+ and sets the \target to +nil+.
      def reset
        @loaded = false
        @target = nil
      end

      # Reloads the \target and returns +self+ on success.
      def reload
        reset
        reset_scope
        load_target
        self unless target.nil?
      end

      # Asserts the \target has been loaded setting the \loaded flag to +true+.
      def loaded!
        @loaded = true
        @stale_state = stale_state
      end

      # The target is stale if the target no longer points to the record(s) that the
      # relevant foreign_key(s) refers to. If stale, the association accessor method
      # on the owner will reload the target. It's up to subclasses to implement the
      # stale_state method if relevant.
      #
      # Note that if the target has not been loaded, it is not considered stale.
      def stale_target?
        loaded? && @stale_state != stale_state
      end

      # Sets the target of this association to <tt>\target</tt>, and the \loaded flag to +true+.
      def target=(target)
        @target = target
        loaded!
      end

      def scoped
        target_scope.merge(association_scope)
      end

      # The scope for this association.
      #
      # Note that the association_scope is merged into the target_scope only when the
      # scoped method is called. This is because at that point the call may be surrounded
      # by scope.scoping { ... } or with_scope { ... } etc, which affects the scope which
      # actually gets built.
      def association_scope
        @association_scope ||= AssociationScope.new(self).scope if klass
      end

      def reset_scope
        @association_scope = nil
      end

      # Set the inverse association, if possible
      def set_inverse_instance(record) # rubocop:disable Style/AccessorMethodName
        return unless record && invertible_for?(record)
        inverse = record.association(inverse_reflection_for(record).name)
        inverse.target = owner
      end

      # This class of the target. belongs_to polymorphic overrides this to look at the
      # polymorphic_type field on the owner.
      def klass
        reflection.klass
      end

      # Can be overridden (i.e. in ThroughAssociation) to merge in other scopes (i.e. the
      # through association's scope)
      def target_scope
        klass.scoped
      end

      # Loads the \target if needed and returns it.
      #
      # This method is abstract in the sense that it relies on +find_target+,
      # which is expected to be provided by descendants.
      #
      # If the \target is already \loaded it is just returned. Thus, you can call
      # +load_target+ unconditionally to get the \target.
      #
      # DatastaxRails::RecordNotFound is rescued within the method, and it is
      # not reraised. The proxy is \reset and +nil+ is the return value.
      def load_target
        @target ||= find_target if find_target?
        loaded! unless loaded?
        target
      rescue DatastaxRails::RecordNotFound
        reset
      end

      private

      def find_target?
        !loaded? && (!owner.new_record? || foreign_key_present?) && klass
      end

      def creation_attributes
        {}.tap do |attributes|
          if reflection.macro.in?([:has_one, :has_many]) && !options[:through]
            attributes[reflection.foreign_key] = owner.id

            # Note, polymorphic relationships are not implemented yet
            if reflection.options[:as]
              attributes[reflection.type] = owner.class.base_class.name
            end
          end
        end
      end

      # Sets the owner attributes on the given record
      def set_owner_attributes(record) # rubocop:disable Style/AccessorMethodName
        creation_attributes.each { |key, value| record[key] = value }
      end

      # Should be true if there is a foreign key present on the owner which
      # references the target. This is used to determine whether we can load
      # the target if the owner is currently a new record (and therefore
      # without a key).
      #
      # Currently implemented by belongs_to (vanilla and polymorphic) and
      # has_one/has_many :through associations which go through a belongs_to
      def foreign_key_present?
        false
      end

      # Raises DatastaxRails::AssociationTypeMismatch unless +record+ is of
      # the kind of the class of the associated objects. Meant to be used as
      # a sanity check when you are about to assign an associated record.
      def raise_on_type_mismatch(record)
        return if record.is_a?(reflection.klass) || record.is_a?(reflection.class_name.constantize)
        message = "#{reflection.class_name}(##{reflection.klass.object_id}) expected," \
                  " got #{record.class}(##{record.class.object_id})"
        fail DatastaxRails::AssociationTypeMismatch, message
      end

      # Can be redefined by subclasses, notably polymorphic belongs_to
      # The record parameter is necessary to support polymorphic inverses as we must check for
      # the association in the specific class of the record.
      def inverse_reflection_for(_record)
        reflection.inverse_of
      end

      # Is this association invertible? Can be redefined by subclasses.
      def invertible_for?(record)
        inverse_reflection_for(record)
      end

      # This should be implemented to return the values of the relevant key(s) on the owner,
      # so that when state_state is different from the value stored on the last find_target,
      # the target is stale.
      #
      # This is only relevant to certain associations, which is why it returns nil by default.
      def stale_state
        nil
      end

      def association_class
        @reflection.klass
      end

      def build_record(attributes, options)
        reflection.build_association(attributes, options) do |r|
          r.assign_attributes(create_scope.except(*r.changed))
        end
      end
    end
  end
end
