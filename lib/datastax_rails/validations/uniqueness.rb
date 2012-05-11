require 'active_support/core_ext/array/wrap'

module DatastaxRails
  module Validations
    class UniquenessValidator < ActiveModel::EachValidator
      def initialize(options)
        super
      end
      
      def validate_each(record, attribute, value)
        # XXX: The following will break if/when abstract base classes
        #      are implemented in solandra object (such as STI)
        finder_class = record.class
        
        scope = finder_class.where_not(:id => record.id.to_s).where(attribute => value)
        Array.wrap(options[:scope]).each do |scope_item|
          scope_value = record.send(scope_item)
          scope_value = nil if scope_value.blank?
          scope = scope.where(scope_item, scope_value)
        end
        if scope.count > 0
          record.errors.add(attribute, "has already been taken", options.except(:case_sensitive, :scope).merge(:value => value))
        end
      end
      
      # def check_validity!
#         
      # end
    end
    
    module ClassMethods
      def validates_uniqueness_of(*attr_names)
        validates_with UniquenessValidator, _merge_attributes(attr_names)
      end
    end
  end
end