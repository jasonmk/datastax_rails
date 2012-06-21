require 'active_support/core_ext/array/wrap'

module DatastaxRails
  module Validations
    class UniquenessValidator < ActiveModel::EachValidator
      def initialize(options)
        super
      end
      
      def setup(klass)
        @klass = klass
      end
      
      def validate_each(record, attribute, value)
        return true if options[:allow_blank] && value.blank?
        # XXX: The following will break if/when abstract base classes
        #      are implemented in datastax_rails (such as STI)
        finder_class = record.class
        
        scope = finder_class.unscoped.where(attribute => value)
        scope = scope.where_not(:id => record.id) if record.persisted?
        
        Array.wrap(options[:scope]).each do |scope_item|
          scope_value = record.send(scope_item)
          scope_value = nil if scope_value.blank?
          scope = scope.where(scope_item => scope_value)
        end

        if scope.exists?
          record.errors.add(attribute, :taken, options.except(:case_sensitive, :scope).merge(:value => value))
        end
      end
    end
    
    module ClassMethods
      # Validates whether the value of the specified attributes are unique across the system.
      # Useful for making sure that only one user can be named "davidhh".
      #
      #   class Person < DatastaxRails::Base
      #     validates_uniqueness_of :user_name
      #   end
      #
      # It can also validate whether the value of the specified attributes are unique based on a scope parameter:
      #
      #   class Person < DatastaxRails::Base
      #     validates_uniqueness_of :user_name, :scope => :account_id
      #   end
      #
      # Or even multiple scope parameters. For example, making sure that a teacher can only be on the schedule once
      # per semester for a particular class.
      #
      #   class TeacherSchedule < DatastaxRails::Base
      #     validates_uniqueness_of :teacher_id, :scope => [:semester_id, :class_id]
      #   end
      #
      # When the record is created, a check is performed to make sure that no record exists in the database
      # with the given value for the specified attribute (that maps to a column). When the record is updated,
      # the same check is made but disregarding the record itself.
      #
      # Configuration options:
      # * <tt>:message</tt> - Specifies a custom error message (default is: "has already been taken").
      # * <tt>:scope</tt> - One or more columns by which to limit the scope of the uniqueness constraint.
      # * <tt>:allow_nil</tt> - If set to true, skips this validation if the attribute is +nil+ (default is +false+).
      # * <tt>:allow_blank</tt> - If set to true, skips this validation if the attribute is blank (default is +false+).
      # * <tt>:if</tt> - Specifies a method, proc or string to call to determine if the validation should
      #   occur (e.g. <tt>:if => :allow_validation</tt>, or <tt>:if => Proc.new { |user| user.signup_step > 2 }</tt>).
      #   The method, proc or string should return or evaluate to a true or false value.
      # * <tt>:unless</tt> - Specifies a method, proc or string to call to determine if the validation should
      #   not occur (e.g. <tt>:unless => :skip_validation</tt>, or
      #   <tt>:unless => Proc.new { |user| user.signup_step <= 2 }</tt>). The method, proc or string should
      #   return or evaluate to a true or false value.
      #
      # === Concurrency and integrity
      #
      # Using this validation method in conjunction with DatastaxRails::Base#save
      # does not guarantee the absence of duplicate record insertions, because
      # uniqueness checks on the application level are inherently prone to race
      # conditions. For example, suppose that two users try to post a Comment at
      # the same time, and a Comment's title must be unique. At the database-level,
      # the actions performed by these users could be interleaved in the following manner:
      #
      #               User 1                |                User 2
      # ------------------------------------+--------------------------------------
      # # User 1 checks whether there's     |
      # # already a comment with the title  |
      # # 'My Post'. This is not the case.  |
      # SELECT * FROM comments              |
      # WHERE title = 'My Post'             |
      #                                     |
      #                                     | # User 2 does the same thing and also
      #                                     | # infers that his title is unique.
      #                                     | SELECT * FROM comments
      #                                     | WHERE title = 'My Post'
      #                                     |
      # # User 1 inserts his comment.       |
      # INSERT INTO comments                |
      # (title, content) VALUES             |
      # ('My Post', 'hi!')                  |
      #                                     |
      #                                     | # User 2 does the same thing.
      #                                     | INSERT INTO comments
      #                                     | (title, content) VALUES
      #                                     | ('My Post', 'hello!')
      #                                     |
      #                                     | # ^^^^^^
      #                                     | # Boom! We now have a duplicate
      #                                     | # title!
      #
      # It is left as an exercise of the developer to figure out how to solve
      # this problem at the application level because there is no way to do
      # so generically since Cassandra doesn't support UNIQUE indexes.
      #
      def validates_uniqueness_of(*attr_names)
        validates_with UniquenessValidator, _merge_attributes(attr_names)
      end
    end
  end
end