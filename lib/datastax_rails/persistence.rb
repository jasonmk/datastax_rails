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
        keys = keys.flatten.collect {|k| self.attribute_definitions[self.primary_key].type_cast(k)}
        ActiveSupport::Notifications.instrument("remove.datastax_rails", :column_family => column_family, :key => keys) do
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
      # @param [DatastaxRails::Base] record the record that we are writing
      # @param [Hash] options a hash containing various options
      # @option options [Symbol] :consistency the consistency to set for the Cassandra operation (e.g., ALL)
      def write(record, options = {})
        level = (options[:consistency] || self.default_consistency).to_s.upcase
        if(valid_consistency?(level))
          options[:consistency] = level
        else
          raise ArgumentError, "'#{level}' is not a valid Cassandra consistency level"
        end
        record.id.tap do |key|
          ActiveSupport::Notifications.instrument("insert.datastax_rails", :column_family => column_family, :key => key.to_s, :attributes => record.attributes) do
            if(self.storage_method == :solr)
              write_with_solr(record, options)
            else
              write_with_cql(record, options)
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
        allocate.init_with('attributes' => attributes)
      end
      
      # Encodes the attributes in preparation for storing in cassandra. Calls the coders on the various type classes
      # to do the heavy lifting.
      #
      # @param [DatastaxRails::Base] record the record whose attributes we're encoding
      # @param [Boolean] cql True if we're formatting for CQL, otherwise False
      # @return [Hash] a new hash with attributes encoded for storage
      def encode_attributes(record, cql)
        encoded = {}
        Types::DirtyCollection.ignore_modifications do
          record.changed.each do |column_name|
            value = record.read_attribute(column_name)
            encoded[column_name.to_s] = cql ? attribute_definitions[column_name].type_cast_for_cql3(value) :
                                              attribute_definitions[column_name].type_cast_for_solr(value)
          end
        end
        encoded
      end
      
      private
        def write_with_cql(record, options)
          encoded = encode_attributes(record, true)
          if options[:new_record]
            cql.insert.columns(encoded).using(options[:consistency]).execute
          else
            cql.update(record.id).columns(encoded).using(options[:consistency]).execute
          end
        end
        
        def write_with_solr(record, options)
          encoded = encode_attributes(record, false)
          xml_doc = RSolr::Xml::Generator.new.add(encoded.merge(self.primary_key => record.id.to_s))
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
      self.class.remove(id, options)
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
    def update_attributes(attributes, options = {})
      self.assign_attributes(attributes)
      save
    end

    # Updates its receiver just like +update_attributes+ but calls <tt>save!</tt> instead
    # of +save+, so an exception is raised if the record is invalid.
    def update_attributes!(attributes, options = {})
      self.assign_attributes(attributes)
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
        result = new_record? ? _create_record(options) : _update_record(options)
        result != false
      end

      def _create_record(options)
        # TODO: handle the non-UUID case
        self.id ||= ::Cql::TimeUuid::Generator.new.next
        _write(options)
        @new_record = false
        self.id
      end
    
      def _update_record(options)
        _write(options)
      end
      
      def _write(options) #:nodoc:
        options[:new_record] = new_record?
        return true if changed_attributes.empty?
        self.class.write(self, options)
      end
  end
end
