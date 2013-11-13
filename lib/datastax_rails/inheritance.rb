module DatastaxRails
  module Inheritance
    extend ActiveSupport::Concern
    
    module ClassMethods
      # Determines if one of the attributes passed in is the inheritance column,
      # and if the inheritance column is attr accessible, it initializes an
      # instance of the given subclass instead of the base class.
      def new(*args, &block)
        if abstract_class? || self == Base
          raise NotImplementedError, "#{self} is an abstract class and can not be instantiated."
        end
        # if (attrs = args.first).is_a?(Hash)
          # if subclass = subclass_from_attrs(attrs)
            # return subclass.new(*args, &block)
          # end
        # end
        # Delegate to the original .new
        super
      end
      
      # Returns the class descending directly from DatastaxRails::Base, or
      # an abstract class, if any, in the inheritance hierarchy.
      #
      # If A extends AR::Base, A.base_class will return A. If B descends from A
      # through some arbitrarily deep hierarchy, B.base_class will return A.
      #
      # If B < A and C < B and if A is an abstract_class then both B.base_class
      # and C.base_class would return B as the answer since A is an abstract_class.
      def base_class
        unless self < Base
          raise DatastaxRailsError, "#{name} doesn't belong in a hierarchy descending from DatastaxRails"
        end

        if superclass == Base || superclass.abstract_class?
          self
        else
          superclass.base_class
        end
      end
      
      # Set this to true if this is an abstract class (see <tt>abstract_class?</tt>).
      # If you are using inheritance with DatastaxRails and don't want child classes
      # to utilize the implied STI table name of the parent class, this will need to be true.
      # For example, given the following:
      #
      #   class SuperClass < DatastaxRails::Base
      #     self.abstract_class = true
      #   end
      #   class Child < SuperClass
      #     self.column_family = 'the_table_i_really_want'
      #   end
      attr_accessor :abstract_class

      # Returns whether this class is an abstract class or not.
      def abstract_class?
        defined?(@abstract_class) && @abstract_class == true
      end
    end
  end
end