require 'active_support/core_ext/array/wrap'
require 'active_support/core_ext/object/inclusion'

module DatastaxRails
  module Associations
    class Association #:nodoc:
      attr_reader :owner, :target, :reflection
      
      delegate :options, :to => :reflection
      
      def initialize(owner, reflection)
        reflection.check_validity!

        @target = nil
        @owner, @reflection = owner, reflection
        @updated = false

        reset
      end
      
      def aliased_column_family
        reflection.klass.column_family
      end
      
      def reset
        @loaded = false
        @target = nil
      end
      
      def reload
        reset
        load_target
        self unless target.nil?
      end
      
      def loaded?
        @loaded
      end
      
      def loaded!
        @loaded = true
        @stale_state = stale_state
      end
      
      # The target is stale if the target no longer points to the record(s) that the
      # relevant foreign_key(s) refers to. If stale, the association accessor method
      # on the owner will reload the target. It's up to subclasses to implement the
      # state_state method if relevant.
      #
      # Note that if the target has not been loaded, it is not considered stale.
      def stale_target?
        loaded? && @stale_state != stale_state
      end
      
      def target=(target)
        @target = target
        loaded!
      end
      
      def scoped
        target_scope.where(reflection.foreign_key => owner.id)
      end
      
      def reset_scope
        @association_scope = nil
      end
      
      def set_inverse_instance(record)
        if record && invertible_for?(record)
          inverse = record.association(inverse_reflection_for(record).name)
          inverse.target = owner
        end
      end
      
      def klass
        reflection.klass
      end
      
      def target_scope
        klass.scoped
      end
      
      def load_target
        if find_target?
          @target ||= find_target
        end
        loaded! unless loaded?
        target
      rescue CassandraObject::RecordNotFound
        reset
      end

      private
        def find_target?
          !loaded? && (!owner.new_record? || foreign_key_present?) && klass
        end
        
        def creation_attributes
          {}.tap do |attributes|
            if reflection.macro.in?([:has_one, :has_many])
              attributes[reflection.foreign_key] = owner.id
            end
          end
        end
        
        def set_owner_attributes(record)
          creation_attriubutes.each { |key, value| record[key] = value }
        end
        
        # Overridden by belongs_to
        def foreign_key_present?
          false
        end
        
        def raise_on_type_mismatch(record)
          unless record.is_a?(reflection.klass) || record.is_a?(reflection.class_name.constantize)
            message = "#{reflection.class_name}(##{reflection.klass.object_id}) expected, got #{record.class}(##{record.class.object_id})"
            raise DatastaxRails::AssociationTypeMismatch, message
          end
        end
        
        def inverse_reflection_for(record)
          reflection.inverse_of
        end
        
        def invertible_for?(record)
          inverse_reflection_for(record)
        end
        
        # Only relevant for certain associations
        def stale_state
          nil
        end
        
        def association_class
          @reflection.klass
        end

        def build_record(attributes, options)
          reflection.build_association(attributes, options)
        end
    end
  end
end
