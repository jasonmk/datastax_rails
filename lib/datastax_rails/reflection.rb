require 'active_support/core_ext/class/attribute'
require 'active_support/core_ext/object/inclusion'

# This is shamelessly ripped from Active Record
module DatastaxRails
  # = DatastaxRails Reflection
  module Reflection # :nodoc:
    extend ActiveSupport::Concern

    included do
      class_attribute :reflections
      self.reflections = {}
    end

    # Reflection enables interrogation of DatastaxRails classes and objects
    # about their associations and aggregations. This information can,
    # for example, be used in a form builder that takes an DatastaxRails object
    # and creates input fields for all of the attributes depending on their type
    # and displays the associations to other objects.
    module ClassMethods
      def create_reflection(macro, name, options, datastax_rails)
        AssociationReflection.new(macro, name, options, datastax_rails).tap do |reflection|
          self.reflections = self.reflections.merge(name => reflection)
        end
      end

      # Returns an array of AssociationReflection objects for all the
      # associations in the class. If you only want to reflect on a certain
      # association type, pass in the symbol (<tt>:has_many</tt>, <tt>:has_one</tt>,
      # <tt>:belongs_to</tt>) as the first parameter.
      #
      # Example:
      #
      #   Account.reflect_on_all_associations             # returns an array of all associations
      #   Account.reflect_on_all_associations(:has_many)  # returns an array of all has_many associations
      #
      def reflect_on_all_associations(macro = nil)
        association_reflections = reflections.values.grep(AssociationReflection)
        macro ? association_reflections.select { |reflection| reflection.macro == macro } : association_reflections
      end

      # Returns the AssociationReflection object for the +association+ (use the symbol).
      #
      #   Account.reflect_on_association(:owner)             # returns the owner AssociationReflection
      #   Invoice.reflect_on_association(:line_items).macro  # returns :has_many
      #
      def reflect_on_association(association)
        reflections[association].is_a?(AssociationReflection) ? reflections[association] : nil
      end

      # Returns an array of AssociationReflection objects for all associations which have <tt>:autosave</tt> enabled.
      def reflect_on_all_autosave_associations
        reflections.values.select { |reflection| reflection.options[:autosave] }
      end
    end


    # Abstract base class for AggregateReflection and AssociationReflection. Objects of
    # AggregateReflection and AssociationReflection are returned by the Reflection::ClassMethods.
    class MacroReflection
      # Returns the name of the macro.
      #
      # <tt>composed_of :balance, :class_name => 'Money'</tt> returns <tt>:balance</tt>
      # <tt>has_many :clients</tt> returns <tt>:clients</tt>
      attr_reader :name

      # Returns the macro type.
      #
      # <tt>composed_of :balance, :class_name => 'Money'</tt> returns <tt>:composed_of</tt>
      # <tt>has_many :clients</tt> returns <tt>:has_many</tt>
      attr_reader :macro

      # Returns the hash of options used for the macro.
      #
      # <tt>composed_of :balance, :class_name => 'Money'</tt> returns <tt>{ :class_name => "Money" }</tt>
      # <tt>has_many :clients</tt> returns +{}+
      attr_reader :options

      attr_reader :datastax_rails

      attr_reader :plural_name # :nodoc:

      def initialize(macro, name, options, datastax_rails)
        @macro           = macro
        @name            = name
        @options         = options
        @datastax_rails = solandra_object
        @plural_name     = name.to_s.pluralize
      end

      # Returns the class for the macro.
      #
      # <tt>composed_of :balance, :class_name => 'Money'</tt> returns the Money class
      # <tt>has_many :clients</tt> returns the Client class
      def klass
        @klass ||= class_name.constantize
      end

      # Returns the class name for the macro.
      #
      # <tt>composed_of :balance, :class_name => 'Money'</tt> returns <tt>'Money'</tt>
      # <tt>has_many :clients</tt> returns <tt>'Client'</tt>
      def class_name
        @class_name ||= (options[:class_name] || derive_class_name).to_s
      end

      # Returns +true+ if +self+ and +other_aggregation+ have the same +name+ attribute, +datastax_rails+ attribute,
      # and +other_aggregation+ has an options hash assigned to it.
      def ==(other_aggregation)
        super ||
          other_aggregation.kind_of?(self.class) &&
          name == other_aggregation.name &&
          other_aggregation.options &&
          active_record == other_aggregation.active_record
      end

      # XXX: Do we need to sanitize our query?
      def sanitized_conditions #:nodoc:
        @sanitized_conditions ||= options[:conditions]
      end

      private
        def derive_class_name
          name.to_s.camelize
        end
    end

    # Holds all the meta-data about an association as it was specified in the
    # DatastaxRails class.
    class AssociationReflection < MacroReflection #:nodoc:
      # Returns the target association's class.
      #
      #   class Author < DatastaxRails::Base
      #     has_many :books
      #   end
      #
      #   Author.reflect_on_association(:books).klass
      #   # => Book
      #
      # <b>Note:</b> Do not call +klass.new+ or +klass.create+ to instantiate
      # a new association object. Use +build_association+ or +create_association+
      # instead. This allows plugins to hook into association object creation.
      def initialize(macro, name, options, datastax_rails)
        super
        @collection = macro.in?([:has_many, :has_and_belongs_to_many])
      end

      # Returns a new, unsaved instance of the associated class. +options+ will
      # be passed to the class's constructor.
      def build_association(*options, &block)
        klass.new(*options, &block)
      end

      def column_family
        @table_name ||= klass.column_family
      end

      def quoted_column_family
        column_family
      end

      def foreign_key
        @foreign_key ||= options[:foreign_key] || derive_foreign_key
      end

      def foreign_type
        @foreign_type ||= options[:foreign_type] || "#{name}_type"
      end

      def type
        @type ||= options[:as] && "#{options[:as]}_type"
      end

      def association_foreign_key
        @association_foreign_key ||= options[:association_foreign_key] || class_name.foreign_key
      end

      # klass option is necessary to support loading polymorphic associations
      # def association_primary_key(klass = nil)
        # options[:primary_key] || primary_key(klass || self.klass)
      # end
# 
      # def datastax_rails_primary_key
        # @datastax_rails_primary_key ||= options[:primary_key] || primary_key(solandra_object)
      # end

      def check_validity!
        check_validity_of_inverse!
      end

      def check_validity_of_inverse!
        unless options[:polymorphic]
          if has_inverse? && inverse_of.nil?
            raise InverseOfAssociationNotFoundError.new(self)
          end
        end
      end

      def through_reflection
        nil
      end

      def source_reflection
        nil
      end

      # A chain of reflections from this one back to the owner. For more see the explanation in
      # ThroughReflection.
      def chain
        [self]
      end

      # An array of arrays of conditions. Each item in the outside array corresponds to a reflection
      # in the #chain. The inside arrays are simply conditions (and each condition may itself be
      # a hash, array, arel predicate, etc...)
      def conditions
        [[options[:conditions]].compact]
      end

      alias :source_macro :macro

      def has_inverse?
        @options[:inverse_of]
      end

      def inverse_of
        if has_inverse?
          @inverse_of ||= klass.reflect_on_association(options[:inverse_of])
        end
      end

      # Returns whether or not this association reflection is for a collection
      # association. Returns +true+ if the +macro+ is either +has_many+ or
      # +has_and_belongs_to_many+, +false+ otherwise.
      def collection?
        @collection
      end

      # Returns whether or not the association should be validated as part of
      # the parent's validation.
      #
      # Unless you explicitly disable validation with
      # <tt>:validate => false</tt>, validation will take place when:
      #
      # * you explicitly enable validation; <tt>:validate => true</tt>
      # * you use autosave; <tt>:autosave => true</tt>
      # * the association is a +has_many+ association
      def validate?
        !options[:validate].nil? ? options[:validate] : (options[:autosave] == true || macro == :has_many)
      end

      # Returns +true+ if +self+ is a +belongs_to+ reflection.
      def belongs_to?
        macro == :belongs_to
      end

      def association_class
        case macro
        when :belongs_to
          Associations::BelongsToAssociation
        when :has_and_belongs_to_many
          Associations::HasAndBelongsToManyAssociation
        when :has_many
          Associations::HasManyAssociation
        when :has_one
          Associations::HasOneAssociation
        end
      end

      private
        def derive_class_name
          class_name = name.to_s.camelize
          class_name = class_name.singularize if collection?
          class_name
        end

        def derive_foreign_key
          if belongs_to?
            "#{name}_id"
          elsif options[:as]
            "#{options[:as]}_id"
          else
            datastax_rails.name.foreign_key
          end
        end

        # def primary_key(klass)
          # klass.key || raise(UnknownPrimaryKey.new(klass))
        # end
    end
  end
end
