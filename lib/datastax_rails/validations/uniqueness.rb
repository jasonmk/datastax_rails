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
        
        count = finder_class.search_ids do
          with attribute, value
          Array.wrap(options[:scope]).each do |scope_item|
            scope_value = record.send(scope_item)
            scope_value = nil if scope_value.blank?
            with scope_item, scope_value
          end
        end.reject {|id| id == record.id}.size
        if count > 0
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