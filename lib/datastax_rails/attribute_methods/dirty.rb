module DatastaxRails
  module AttributeMethods
    module Dirty
      extend ActiveSupport::Concern
      include ActiveModel::Dirty

      included do
        if self < ::DatastaxRails::Timestamps
          fail 'You cannot include Dirty after Timestamps'
        end
      end

      # Attempts to +save+ the record and clears changed attributes if successful.
      def save(*) #:nodoc:
        if (status = super)
          @previously_changed = changes
          @changed_attributes.clear
        end
        status
      end

      # Attempts to <tt>save!</tt> the record and clears changed attributes if successful.
      def save!(*) #:nodoc:
        super.tap do
          @previously_changed = changes
          @changed_attributes.clear
        end
      end

      # <tt>reload</tt> the record and clears changed attributes.
      def reload(*)
        super.tap do
          @previously_changed.clear
          @changed_attributes.clear
        end
      end

      def write_attribute(attr, value)
        attr = attr.to_s
        loaded_attributes[attr] = true

        # The attribute already has an unsaved change.
        if attribute_changed?(attr)
          old = @changed_attributes[attr]
          @changed_attributes.delete(attr) unless _field_changed?(attr, old, value)
        else
          old = clone_attribute_value(:read_attribute, attr)
          @changed_attributes[attr] = old if _field_changed?(attr, old, value)
        end

        super
      end

      private

      def _field_changed?(attr, old, value)
        if (column = column_for_attribute(attr))
          if column.number? && (changes_from_nil_to_empty_string?(column, old, value) ||
                                changes_from_zero_to_string?(old, value))
            value = nil
          else
            value = column.type_cast(value, self)
          end
        end

        old != value
      end

      def changes_from_nil_to_empty_string?(_column, old, value)
        # We don't record it as a change if the value changes from nil to ''.
        # If an old value of 0 is set to '' we want this to get changed to nil as otherwise it'll
        # be typecast back to 0 (''.to_i => 0)
        (old.nil? || old == 0) && value.blank?
      end

      def changes_from_zero_to_string?(old, value)
        # For columns with old 0 and value non-empty string
        old == 0 && value.is_a?(String) && value.present? && non_zero?(value)
      end

      def non_zero?(value)
        value !~ /\A0+(\.0+)?\z/
      end
    end
  end
end
