require 'active_support/concern'

module DatastaxRails
  module AttributeAssignment
    extend ActiveSupport::Concern
    include ActiveModel::ForbiddenAttributesProtection

    # Allows you to set all the attributes by passing in a hash of attributes with
    # keys matching the attribute names (which again matches the column names).
    #
    # If the passed hash responds to <tt>permitted?</tt> method and the return value
    # of this method is +false+ an <tt>ActiveModel::ForbiddenAttributesError</tt>
    # exception is raised.
    def assign_attributes(new_attributes)
      return if new_attributes.blank?

      attributes = new_attributes.stringify_keys
      nested_parameter_attributes = []

      attributes = sanitize_for_mass_assignment(attributes)

      attributes.each do |k, v|
        if v.is_a?(Hash)
          nested_parameter_attributes << [ k, v ]
        else
          _assign_attribute(k, v)
        end
      end

      assign_nested_parameter_attributes(nested_parameter_attributes) unless nested_parameter_attributes.empty?
    end
    alias attributes= assign_attributes
    
    private

      def _assign_attribute(k, v)
        public_send("#{k}=", v)
      rescue NoMethodError
        if respond_to?("#{k}=")
          raise
        else
          raise UnknownAttributeError, "unknown attribute: #{k}"
        end
      end
  
      # Assign any deferred nested attributes after the base attributes have been set.
      def assign_nested_parameter_attributes(pairs)
        pairs.each { |k, v| _assign_attribute(k, v) }
      end
  end
end
