require 'set'

module DatastaxRails
  module AttributeMethods
    module PrimaryKey
      extend ActiveSupport::Concern

      # Returns this record's primary key value wrapped in an Array if one is
      # available.
      def to_key
        key = id
        [key] if key
      end

      # Returns a primary key hash for updates. Wide models override this.
      def id_for_update
        { self.class.primary_key.to_s => __id }
      end

      # Returns the primary key value.
      def id
        read_attribute(self.class.primary_key)
      end

      def __id
        self.class.column_for_attribute(self.class.primary_key).type_cast_for_cql3(id)
      end

      # Sets the primary key value.
      def id=(value)
        write_attribute(self.class.primary_key, value) if self.class.primary_key
      end

      # Queries the primary key value.
      def id?
        query_attribute(self.class.primary_key)
      end

      protected

      def attribute_method?(attr_name)
        attr_name == 'id' || super
      end

      module ClassMethods
        def define_method_attribute(attr_name)
          super

          if attr_name == primary_key && attr_name != 'id'
            generated_attribute_methods.send(:alias_method, :id, primary_key)
          end
        end

        ID_ATTRIBUTE_METHODS = %w(id id= id? id_before_type_cast).to_set

        # Defines the primary key field -- can be overridden in subclasses.
        # Overwriting will negate any effect of the +primary_key_prefix_type+
        # setting, though.
        def primary_key
          @primary_key || 'id'
        end

        # Returns a quoted version of the primary key name, used to construct
        # CQL statements.
        def quoted_primary_key
          @quoted_primary_key ||= connection.quote_column_name(primary_key)
        end

        # Sets the name of the primary key column.
        #
        #   class Project < DatastaxRails::Base
        #     self.primary_key = 'sysid'
        #   end
        #
        # You can also define the +primary_key+ method yourself:
        #
        #   class Project < DatastaxRails::Base
        #     def self.primary_key
        #       'foo_' + super
        #     end
        #   end
        #
        #   Project.primary_key # => "foo_id"
        def primary_key=(value)
          @primary_key        = value && value.to_s
          @quoted_primary_key = nil
        end
      end
    end
  end
end
