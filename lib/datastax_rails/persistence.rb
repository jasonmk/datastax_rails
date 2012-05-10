module DatastaxRails
  module Persistence
    extend ActiveSupport::Concern
    
    module ClassMethods
      def remove(*keys)
        ActiveSupport::Notifications.instrument("remove.datastax_rails", :column_family => column_family, :key => key) do
          cql.delete(keys).using(thrift_write_consistency).execute
        end
      end

      def delete_all
        ActiveSupport::Notifications.instrument("truncate.datastax_rails", :column_family => column_family) do
          cql.truncate.execute
        end
      end

      def create(attributes = {})
        new(attributes).tap do |object|
          object.save
        end
      end

      def write(key, attributes, schema_version)
        key.tap do |key|
          attributes = encode_attributes(attributes, schema_version)
          ActiveSupport::Notifications.instrument("insert.datastax_rails", :column_family => column_family, :key => key, :attributes => attributes) do
            cql.update(key.to_s).columns(attributes).using(thrift_write_consistency).execute
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

    def save(*)
      begin
        create_or_update
      rescue DatastaxRails::RecordInvalid
        false
      end
    end

    def save!
      create_or_update || raise(RecordNotSaved)
    end

    def destroy
      self.class.remove(key)
      @destroyed = true
      freeze
    end

    def update_attribute(name, value)
      name = name.to_s
      send("#{name}=", value)
      save(:validate => false)
    end

    def update_attributes(attributes)
      self.attributes = attributes
      save
    end

    def update_attributes!(attributes)
      self.attributes = attributes
      save!
    end

    def reload
      @attributes.update(self.class.find(self.id).instance_variable_get('@attributes'))
    end

    private
      def create_or_update
        result = new_record? ? create : update
        result != false
      end

      def create
        @key ||= self.class.next_key(self)
        write
        @new_record = false
        @key
      end
    
      def update
        write
      end
      
      def write #:nodoc:
        changed_attributes = changed.inject({}) { |h, n| h[n] = read_attribute(n); h }
        self.class.write(key, changed_attributes, schema_version)
      end
  end
end