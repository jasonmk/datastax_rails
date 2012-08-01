module DatastaxRails
  module Persistence
    extend ActiveSupport::Concern
    
    included do
      attr_accessor :ds_consistency_level
    end
    
    module ClassMethods
      # Removes one or more records with corresponding keys.  Last parameter can be a hash
      # specifying the consistency level.
      #
      #   Model.remove('12345','67890', :consistency => 'LOCAL_QUORUM)
      #
      # @overload remove(*keys, options)
      #   Removes one or more keys with the given options
      #   @param [String] keys one or more keys to delete
      #   @param [Hash] options generally the consistency level to set
      # @overload remove(*keys)
      #   Removes one or more keys with the default options
      #   @param [String] keys one or more keys to delete
      def remove(*keys)
        options = {}
        if keys.last.is_a?(Hash)
          options = keys.pop
        end
        ActiveSupport::Notifications.instrument("remove.datastax_rails", :column_family => column_family, :key => key) do
          c = cql.delete(keys)
          if(options[:consistency])
            level = options[:consistency].to_s.upcase
            if(valid_consistency?(level))
              c.using(level)
            else
              raise ArgumentError, "'#{level}' is not a valid Cassandra consistency level"
            end
          end
          c.execute
        end
      end

      # Truncates the column_family associated with this class
      def truncate
        ActiveSupport::Notifications.instrument("truncate.datastax_rails", :column_family => column_family) do
          cql.truncate.execute
        end
      end
      alias :delete_all :truncate

      def create(attributes = {}, options = {}, &block)
        new(attributes, &block).tap do |object|
          object.save(options)
        end
      end
      
      # Write a record to cassandra.  Can be either an insert or an update (they are exactly the same to cassandra)
      #
      # @param [String] key the primary key for the record
      # @param [Hash] attributes a hash containing the columns to set on the record
      # @param [Hash] options a hash containing various options
      # @option options [Symbol] :consistency the consistency to set for the Cassandra operation (e.g., ALL)
      # @option options [String] :schema_version the version of the schema to set for this record
      def write(key, attributes, options = {})
        key.tap do |key|
          attributes = encode_attributes(attributes, options[:schema_version])
          ActiveSupport::Notifications.instrument("insert.datastax_rails", :column_family => column_family, :key => key, :attributes => attributes) do
            c = cql.update(key.to_s).columns(attributes)
            if(options[:consistency])
              level = options[:consistency].to_s.upcase
              if(valid_consistency?(level))
                c.using(options[:consistency])
              else
                raise ArgumentError, "'#{level}' is not a valid Cassandra consistency level"
              end
            end
            c.execute
          end
        end
      end

      def instantiate(key, attributes)
        allocate.tap do |object|
          object.instance_variable_set("@schema_version", attributes.delete('schema_version'))
          object.instance_variable_set("@key", parse_key(key)) if key
          object.instance_variable_set("@new_record", false)
          object.instance_variable_set("@destroyed", false)
          object.instance_variable_set("@attributes", typecast_attributes(object, attributes))
        end
      end

      def encode_attributes(attributes, schema_version)
        encoded = {"schema_version" => schema_version.to_s}
        attributes.each do |column_name, value|
          if value.nil?
            encoded[column_name.to_s] = ""
          else
            encoded[column_name.to_s] = attribute_definitions[column_name.to_sym].coder.encode(value)
          end
        end
        encoded
      end

      def typecast_attributes(object, attributes)
        attributes = attributes.symbolize_keys
        Hash[attribute_definitions.map { |k, attribute_definition| [k.to_s, attribute_definition.instantiate(object, attributes[k])] }]
      end
    end

    def new_record?
      @new_record
    end

    def destroyed?
      @destroyed
    end

    def persisted?
      !(new_record? || destroyed?)
    end

    def save(options = {})
      begin
        create_or_update(options)
      rescue DatastaxRails::RecordInvalid
        false
      end
    end

    def save!(options = {})
      create_or_update(options) || raise(RecordNotSaved)
    end

    def destroy(options = {})
      self.class.remove(key, options)
      @destroyed = true
      freeze
    end

    def update_attribute(name, value, options = {})
      name = name.to_s
      send("#{name}=", value)
      save(options.merge(:validate => false))
    end

    def update_attributes(attributes, options = {})
      self.attributes = attributes
      save(options)
    end

    def update_attributes!(attributes, options = {})
      self.attributes = attributes
      save!(options)
    end

    def reload
      @attributes.update(self.class.find(self.id).instance_variable_get('@attributes'))
    end

    private
      def create_or_update(options)
        result = new_record? ? create(options) : update(options)
        result != false
      end

      def create(options)
        @key ||= self.class.next_key(self)
        write(options)
        @new_record = false
        @key
      end
    
      def update(options)
        write(options)
      end
      
      def write(options) #:nodoc:
        changed_attributes = changed.inject({}) { |h, n| h[n] = read_attribute(n); h }
        self.class.write(key, changed_attributes, options.merge(:schema_version => schema_version))
      end
  end
end