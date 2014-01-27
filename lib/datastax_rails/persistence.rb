module DatastaxRails
  module Persistence
    extend ActiveSupport::Concern
    
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
      def write(key, attributes, options = {})
        level = (options[:consistency] || self.default_consistency).to_s.upcase
        if(valid_consistency?(level))
          options[:consistency] = level
        else
          raise ArgumentError, "'#{level}' is not a valid Cassandra consistency level"
        end
        key.tap do |key|
          ActiveSupport::Notifications.instrument("insert.datastax_rails", :column_family => column_family, :key => key, :attributes => attributes) do
            if(self.storage_method == :solr)
              write_with_solr(key, attributes, options)
            else
              write_with_cql(key, attributes, options)
            end
          end
        end
      end
      
      # Instantiates a new object without calling +initialize+.
      #
      # @param [String] key the primary key for the record
      # @param [Hash] attributes a hash containing the columns to set on the record
      # @param [Array] selected_attributes an array containing the attributes that were originally selected from cassandra
      #   to build this object.  Used so that we can avoid lazy-loading attributes that don't exist.
      # @return [DatastaxRails::Base] a model with the given attributes
      def instantiate(key, attributes, selected_attributes = [])
        allocate.tap do |object|
          object.instance_variable_set("@loaded_attributes", {}.with_indifferent_access)
          object.instance_variable_set("@key", parse_key(key)) if key
          object.instance_variable_set("@new_record", false)
          object.instance_variable_set("@destroyed", false)
          object.instance_variable_set("@attributes", typecast_attributes(object, attributes, selected_attributes).with_indifferent_access)
        end
      end

      # Encodes the attributes in preparation for storing in cassandra. Calls the coders on the various type classes
      # to do the heavy lifting.
      #
      # @param [Hash] attributes a hash containing the attributes to be encoded for storage
      # @return [Hash] a new hash with attributes encoded for storage
      def encode_attributes(attributes)
        encoded = {}
        attributes.each do |column_name, value|
            encoded[column_name.to_s] = attribute_definitions[column_name.to_sym].coder.encode(value)
        end
        encoded
      end

      def typecast_attributes(object, attributes, selected_attributes = [])
        attributes = attributes.symbolize_keys
        casted = {}
        
        selected_attributes.each do |att|
          object.loaded_attributes[att] = true
        end
        
        attribute_definitions.each do |k,definition|
          casted[k.to_s] = definition.instantiate(object, attributes[k.to_sym])#.to_s
        end
        casted
      end
      
      private
        def write_with_cql(key, attributes, options)
          attributes = encode_attributes(attributes) if self.legacy_mapping?
          cql.update(key).columns(attributes).using(options[:consistency]).execute
        end
        
        def write_with_solr(key, attributes, options)
          attributes = encode_attributes(attributes)
          xml_doc = RSolr::Xml::Generator.new.add(attributes.merge(:id => key))
          self.solr_connection.update(:data => xml_doc, :params => {:replacefields => false, :cl => options[:consistency]})
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
        _create_or_update(options)
      rescue DatastaxRails::RecordInvalid
        false
      end
    end

    def save!(options = {})
      _create_or_update(options) || raise(RecordNotSaved)
    end

    def destroy(options = {})
      self.class.remove(key, options)
      @destroyed = true
      freeze
    end

    # Updates a single attribute and saves the record.
    # This is especially useful for boolean flags on existing records. Also note that
    #
    # * Validation is skipped.
    # * Callbacks are invoked.
    # * updated_at/updated_on column is updated if that column is available.
    # * Updates all the attributes that are dirty in this object.
    #
    def update_attribute(name, value)
      send("#{name.to_s}=", value)
      save(:validate => false)
    end

    # Updates the attributes of the model from the passed-in hash and saves the
    # record If the object is invalid, the saving will fail and false will be returned.
    #
    # When updating model attributes, mass-assignment security protection is respected.
    # If no +:as+ option is supplied then the +:default+ role will be used.
    # If you want to bypass the protection given by +attr_protected+ and
    # +attr_accessible+ then you can do so using the +:without_protection+ option.
    def update_attributes(attributes, options = {})
      self.assign_attributes(attributes, options)
      save
    end

    # Updates its receiver just like +update_attributes+ but calls <tt>save!</tt> instead
    # of +save+, so an exception is raised if the record is invalid.
    def update_attributes!(attributes, options = {})
      self.assign_attributes(attributes, options)
      save!
    end
    
    # Assigns to +attribute+ the boolean opposite of <tt>attribute?</tt>. So
    # if the predicate returns +true+ the attribute will become +false+. This
    # method toggles directly the underlying value without calling any setter.
    # Returns +self+.
    def toggle(attribute)
      self[attribute] = !send("#{attribute}?")
      self
    end

    # Wrapper around +toggle+ that saves the record. This method differs from
    # its non-bang version in that it passes through the attribute setter.
    # Saving is not subjected to validation checks. Returns +true+ if the
    # record could be saved.
    def toggle!(attribute)
      toggle(attribute).update_attribute(attribute, self[attribute])
    end
    
    # Reloads the attributes of this object from the database.
    # The optional options argument is passed to find when reloading so you
    # may do e.g. record.reload(:lock => true) to reload the same record with
    # an exclusive row lock.
    def reload(options = nil)
      clear_association_cache

      fresh_object = self.class.unscoped { self.class.find(self.id, options) }
      @attributes.update(fresh_object.instance_variable_get('@attributes'))

      @attributes_cache = {}
      self
    end

    private
      def _create_or_update(options)
        result = new_record? ? _create(options) : _update(options)
        result != false
      end

      def _create(options)
        @key ||= self.class.next_key(self)
        _write(options)
        @new_record = false
        @key
      end
    
      def _update(options)
        _write(options)
      end
      
      def _write(options) #:nodoc:
        options[:new_record] = new_record?
        changed_attributes = changed.inject({}) { |h, n| h[n] = read_attribute(n); h }
        return true if changed_attributes.empty?
        self.class.write(key, changed_attributes, options)
      end
  end
end
