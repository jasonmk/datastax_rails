require 'active_support/concern'

module DatastaxRails
  module AttributeAssignment
    extend ActiveSupport::Concern
    if Rails.version =~ /^3.*/
      include ActiveModel::MassAssignmentSecurity
    elsif Rails.version =~ /^4.*/
      include ActiveModel::DeprecatedMassAssignmentSecurity
      include ActiveModel::ForbiddenAttributesProtection
    end

    module ClassMethods
      private

      # The primary key can never be set by mass-assignment for security reasons.
      def attributes_protected_by_default
        ["key"]
      end
    end

    # Allows you to set all the attributes at once by passing in a hash with keys
    # matching the attribute names (which again matches the column names).
    #
    # If any attributes are protected by either +attr_protected+ or
    # +attr_accessible+ then only settable attributes will be assigned.
    #
    #   class User < ActiveRecord::Base
    #     attr_protected :is_admin
    #   end
    #
    #   user = User.new
    #   user.attributes = { :username => 'Phusion', :is_admin => true }
    #   user.username   # => "Phusion"
    #   user.is_admin?  # => false
    def attributes=(new_attributes)
      return unless new_attributes.is_a?(Hash)

      assign_attributes(new_attributes)
    end

    # Allows you to set all the attributes for a particular mass-assignment
    # security role by passing in a hash of attributes with keys matching
    # the attribute names (which again matches the column names) and the role
    # name using the :as option.
    #
    # To bypass mass-assignment security you can use the :without_protection => true
    # option.
    #
    #   class User < ActiveRecord::Base
    #     attr_accessible :name
    #     attr_accessible :name, :is_admin, :as => :admin
    #   end
    #
    #   user = User.new
    #   user.assign_attributes({ :name => 'Josh', :is_admin => true })
    #   user.name       # => "Josh"
    #   user.is_admin?  # => false
    #
    #   user = User.new
    #   user.assign_attributes({ :name => 'Josh', :is_admin => true }, :as => :admin)
    #   user.name       # => "Josh"
    #   user.is_admin?  # => true
    #
    #   user = User.new
    #   user.assign_attributes({ :name => 'Josh', :is_admin => true }, :without_protection => true)
    #   user.name       # => "Josh"
    #   user.is_admin?  # => true
    def assign_attributes(new_attributes, options = {})
      return if new_attributes.blank?

      attributes = new_attributes.stringify_keys
      nested_parameter_attributes = []
      @mass_assignment_options = options

      unless options[:without_protection]
        if Rails.version =~ /3.*/
          attributes = sanitize_for_mass_assignment(attributes, mass_assignment_role)
        else
          attributes = sanitize_for_mass_assignment(attributes)
        end
      end

      attributes.each do |k, v|
        if respond_to?("#{k}=")
          if v.is_a?(Hash)
            nested_parameter_attributes << [ k, v ]
          else
            send("#{k}=", v)
          end
        else
          raise(UnknownAttributeError, "unknown attribute: #{k}")
        end
      end

      # assign any deferred nested attributes after the base attributes have been set
      nested_parameter_attributes.each do |k,v|
        send("#{k}=", v)
      end

      @mass_assignment_options = nil
    end

    protected

    def mass_assignment_options
      @mass_assignment_options ||= {}
    end

    def mass_assignment_role
      mass_assignment_options[:as] || :default
    end
  end
end
