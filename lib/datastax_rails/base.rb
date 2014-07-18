require 'datastax_rails/errors'
module DatastaxRails #:nodoc:
  # = DatastaxRails
  #
  # DatastaxRails-based objects differ from Active Record objects in that they specify their
  # attributes directly on the model.  This is necessary because of the fact that Cassandra
  # column families do not have a set list of columns but rather can have different columns per
  # row. (This is not strictly true any more, but it's still not as nailed down as SQL.)
  # By specifying the attributes on the model, getters and setters are automatically
  # created, and the attribute is automatically indexed into SOLR.
  #
  #
  # == Primary Keys
  #
  # Several types of primary keys are supported in DSR. The most common type used is UUID.
  # In general, incrementing numbers are not used as there is no way to guarantee a
  # consistent one-up number across nodes. The following will cause a unique UUID to be
  # generated for each model. This works best if you are using the RandomPartitioner in
  # your Datastax cluster.
  #
  #   class Person < DatastaxRails::Base
  #     uuid :id
  #   end
  #
  # You don't have to use a uuid. You can use a different column as your primary key.
  #
  #   class Person < DatastaxRails::Base
  #     self.primary_key = 'userid'
  #     string :userid
  #   end
  #
  # == Attributes
  #
  # Attributes are specified near the top of the model. The following attribute types
  # are supported:
  #
  # * binary - a large object that will not be indexed into SOLR (e.g., BLOB)
  # * boolean - true/false values
  # * date - a date without a time component
  # * float - a number in floating point notation
  # * integer - a whole, round number of any size
  # * list - an ordered list of values of a single type
  # * map - a collection of key/value pairs of a single type (keys are always strings)
  # * set - an un-ordered set of unique values of a single type
  # * string - a generic string type that is not tokenized by default
  # * text - like strings but will be tokenized for full-text searching by default
  # * time - a datetime object
  # * timestamps - a special type that instructs DSR to include created_at and updated_at
  # * uuid - a UUID in standard UUID format
  #
  # The following options may be specified on the various types to control how they
  # are indexed into SOLR:
  #
  # * solr_index - If the attribute should the attribute be indexed into SOLR.
  #   Defaults to true for everything but binary.
  # * solr_store - If the attribute should the attribute be stored in SOLR.
  #   Defaults to true for everything but binary. (see note)
  # * sortable - If the attribute should be sortable by SOLR.
  #   Defaults to true for everything but binary and text. (see note)
  # * tokenized - If the attribute should be tokenized for full-text searching within the field.
  #   Defaults to true for text.
  # * fulltext - If the attribute should be included in the default field for full-text searches.
  #   Defaults to true for text and string.
  # * multi_valued - If the field will contain multiple values in Solr.
  #   Defaults to true for list and set. This should never need to be set manually.
  #
  # NOTES:
  # * No fields are actually stored in SOLR. When a field is requested from SOLR, the field
  #   is retrieved from Cassandra behind the scenes and returned as if it were stored. The
  #   stored parameter actually controls whether SOLR will return the field at all. If a field
  #   is not stored then asking SOLR for it will return a nil value. It will also not be
  #   included in the field list when all (*) fields are requested.
  # * If you want a field both sortable and searchable (e.g., a subject) then declare it a
  #   text field with <tt>:sortable => true</tt>. This will create two copies of the field in SOLR,
  #   one that gets tokenized and one that is a single token for sorting. As this inflates the
  #   size of the index, you don't want to do this for large fields (which probably don't make
  #   sense to sort on anyways).
  #
  # EXAMPLE:
  # 
  #   class Person < DatastaxRails::Base
  #     uuid    :id
  #     string  :first_name
  #     string  :user_name
  #     text    :bio
  #     date    :birthdate
  #     boolean :active
  #     timestamps
  #   end
  #
  # == Schemas
  #
  # DSR will automatically manage both the Cassandra and Solr schemas for you based on the
  # attributes that you specify on the model. You can override the Solr schema if you
  # want to have something custom. There is a rake task that manages all of the schema
  # information. It will create column families and columns as needed and upload the
  # Solr schema when necessary. If there are changes, it will automatically kick off a
  # reindex in the background.
  #
  # As of Cassandra 1.2, there is no way to remove a column. Cassandra 2.0 supports it,
  # but it hasn't been implemented in DSR yet.
  #
  # TODO: Need a way to remove ununsed column families.
  #
  # == Creation
  #
  # DatastaxRails objects accept constructor parameters either in a hash or as a block. The hash
  # method is especially useful when you're receiving the data from somewhere else, like an
  # HTTP request. It works like this:
  #
  #   user = User.new(name: "David", occupation: "Code Artist")
  #   user.name # => "David"
  #
  # You can also use block initialization:
  #
  #   user = User.new do |u|
  #     u.name = "David"
  #     u.occupation = "Code Artist"
  #   end
  #
  # And of course you can just create a bare object and specify the attributes after the fact:
  #
  #   user = User.new
  #   user.name = "David"
  #   user.occupation = "Code Artist"
  #
  # == Consistency
  #
  # Cassandra has a concept of consistency levels when it comes to saving records.  For a
  # detailed discussion on Cassandra data consistency, see:
  # http://www.datastax.com/documentation/cassandra/1.2/cassandra/dml/dml_config_consistency_c.html
  #
  # DatastaxRails allows you to specify the consistency when you save and retrieve objects.
  #
  #   user = User.new(name: 'David')
  #   user.save(consistency: 'ALL')
  #
  #   User.create(params[:user], {consistency: :local_quorum})
  #
  #   User.consistency(:local_quorum).where(name: 'David')
  #
  # The default consistency level in DatastaxRails is QUORUM for writes and for retrieval
  # by ID.  SOLR only supports a consistency level of ONE.  See the documentation for
  # SearchMethods#consistency for a more detailed explanation.
  #
  # The overall default consistency for a given model can be overridden by setting the
  # +default_consistency+ property.
  #
  #   class Model < DatastaxRails::Base
  #     self.default_consistency = :local_quorum
  #   end
  #
  # The default consistency for all models can be selected by setting the property on
  # DatastaxRails::Base.
  #
  #   DatastaxRails::Base.default_consistency = :one
  #
  # == Conditions
  #
  # Conditions are specified as a hash representing key/value pairs that will eventually be passed to SOLR or as
  # a chained call for greater_than and less_than conditions.  In addition, fulltext queries may be specified as a
  # string that will eventually be parsed by SOLR as a standard SOLR query.
  #
  # A simple hash without a statement will generate conditions based on equality using boolean AND logic.
  # For instance:
  #
  #   Student.where(first_name: "Harvey", status: 1)
  #   Student.where(params[:student])
  #
  # A range may be used in the hash to use a SOLR range query:
  #
  #   Student.where(grade: 9..12)
  #
  # An array may be used in the hash to construct a SOLR OR query:
  #
  #   Student.where(grade: [9,11,12])
  #
  # Inequality can be tested for like so:
  #
  #   Student.where_not(grade: 9)
  #   Student.where(:grade).greater_than(9)
  #   Student.where(:grade).less_than(10)
  #
  # NOTE that Solr inequalities are inclusive so really, the second example above is retrieving records
  # where grace is greater than or equal to 9. Be sure to keep this in mind when you do inequality queries.
  #
  # Fulltext searching is natively supported. All string and text fields are automatically indexed for
  # fulltext searching.
  #
  #   Post.fulltext('Apple AND "iPhone 4s"')
  #
  # See the documentation on {DatastaxRails::SearchMethods} for more information and examples.
  #
  # == Overwriting default accessors
  #
  # All column values are automatically available through basic accessors on the object,
  # but sometimes you want to specialize this behavior. This can be done by overwriting
  # the default accessors (using the same name as the attribute) and calling
  # <tt>read_attribute(attr_name)</tt> and <tt>write_attribute(attr_name, value)</tt> to actually
  # change things.
  #
  #   class Song < DatastaxRails::Base
  #     # Uses an integer of seconds to hold the length of the song
  #
  #     def length=(minutes)
  #       write_attribute(:length, minutes.to_i * 60)
  #     end
  #
  #     def length
  #       read_attribute(:length) / 60
  #     end
  #   end
  #
  # You can alternatively use <tt>self[:attribute]=(value)</tt> and <tt>self[:attribute]</tt>
  # instead of <tt>write_attribute(:attribute, value)</tt> and <tt>read_attribute(:attribute)</tt>.
  #
  # == Dynamic attribute-based finders
  #
  # Dynamic finders have been removed from Rails. As a result, they have also been removed from DSR.
  # In its place, the +find_by+ method can be used:
  #
  #   Student.find_by(name: 'Jason')
  #
  # NOTE: there is a subtle difference between the following that does not exist in ActiveRecord:
  #
  #   Student.find_by(name: 'Jason')
  #   Student.where(name: 'Jason').first
  #
  # The difference is that the first is escaped so that special characters can be used. The
  # second method requires you to do the escaping yourself if you need it done. As an example,
  #
  #   Company.find_by(name: 'All*') #=> finds only the company with the literal name 'All*'
  #   Company.where(name: 'All*').first #=> finds the first company whose name begins with All
  #
  # See DatastaxRails::FinderMethods for more information
  # 
  # == Facets
  #
  # DSR support both field and range facets. For additional detail on facets, see the documentation
  # available under the {DatastaxRails::FacetMethods} module. The result is available through the 
  # facets accessor.
  #
  #   results = Article.field_facet(:author)
  #   results.facets #=> {"author"=>["vonnegut", 2. "asimov", 3]} 
  #
  #   Model.field_facet(:author)
  #   Model.field_facet(:author, sort: 'count', limit: 10, mincount: 1)
  #   Model.range_facet(:price, 500, 1000, 10)
  #   Model.range_facet(:price, 500, 1000, 10, include: 'all')
  #   Model.range_facet(:publication_date, "1968-01-01T00:00:00Z", "2000-01-01T00:00:00Z", "+1YEAR")
  #
  # Range Gap syntax for dates: +1YEAR, +5YEAR, +5YEARS, +1MONTH, +1DAY
  #
  # Useful constants:
  #
  # DatastaxRails::FacetMethods::BY_YEAR  (+1YEAR)
  # DatastaxRails::FacetMethods::BY_MONTH (+1MONTH)
  # DatastaxRails::FacetMethods::BY_DAY   (+1DAY)
  #
  # Model.range_facet(:publication_date, "1968-01-01T00:00:00Z", "2000-01-01T00:00:00Z", DatastaxRails::FacetMethods::BY_YEAR)
  #
  # == Collections
  #
  # Cassandra supports the notion of collections on a row. The three types of supported
  # collections are +set+, +list+, and +map+.
  #
  # By default collections hold strings. You can override this by passing a :holds option in the
  # attribute definition. Sets can hold anything other than other collections, however, a given
  # collection can only hold a single type of values.
  #
  # NOTE: There is a limitation in Cassandra where only the first 64k entries of a collection are
  # ever returned with a query. Therefore, if you put more than 64k entries in a collection you
  # will lose data.
  #
  # === Set
  # 
  # A set is an un-ordered collection of unique values. This collection is fully searchable in Solr.
  #
  #   class User < DatastaxRails::Base
  #     uuid   :id
  #     string :username
  #     set    :emails
  #   end
  #
  # The default set will hold strings. You can modify this behavior like so:
  #
  #   class Student < DatastaxRails::Base
  #     uuid   :id
  #     string :name
  #     set    :grades, holds: :integers
  #   end
  #
  #   User.where(emails: 'jim@example.com') #=> Returns all users where jim@example.com is in the set
  #   user = User.new(name: 'Jim', emails: ['jim@example.com'])
  #   user.emails << 'jim@example.com'
  #   user.emails #=> ['jim@example.com']
  #
  # === List
  # 
  # An ordered collection of values. They do not necessarily have to be unique. The collection
  # will be fully searchable in Solr.
  #
  #   class Student < DatastaxRails::Base
  #     uuid :id
  #     string :name
  #     list :classrooms, holds: integers
  #   end
  #
  #   Student.where(classrooms: 307) #=> Returns all students that have a class in room 307.
  #   student = Student.new(name: 'Sally', classrooms: [307, 305, 301, 307])
  #   student.classrooms << 304
  #   student.classrooms #=> [307, 305, 301, 307, 304]
  #
  # === Map
  #
  # A collection of key/value pairs where the key is a string and the value is the
  # specified type. The collection becomes available in Solr as dynamic fields.
  #
  #   class Student < DatastaxRails::Base
  #     uuid :id
  #     string :name
  #     map :scores_, holds: :integers
  #   end
  #
  #   student = Student.new(:name 'Sally')
  #   student.scores['midterm'] = 98
  #   student.scores['final'] = 97
  #   student.scores #=> {'scores_midterm' => 98, 'scores_final' => 97}
  #   Student.where(scores_final: 97) #=> Returns all students that scored 97 on their final
  #
  # Note that the map name gets prepended to the key. This is how Solr maps it's dynamic fields
  # into the cassandra map. For this reason, it's usually a good idea to put an underscore (_)
  # at the end of the map name to prevent collisions.
  #
  # == Exceptions
  #
  # * DatastaxRailsError - Generic error class and superclass of all other errors raised by DatastaxRails.
  # * AssociationTypeMismatch - The object assigned to the association wasn't of the type
  #   specified in the association definition.
  # * ConnectionNotEstablished+ - No connection has been established. Use <tt>establish_connection</tt>
  #   before querying.
  # * RecordNotFound - No record responded to the +find+ method. Either the row with the given ID doesn't exist
  #   or the row didn't meet the additional restrictions. Some +find+ calls do not raise this exception to signal
  #   nothing was found, please check its documentation for further details.
  # * UnknownAttributeError - The specified attribute isn't defined on your model.
  #
  # See the documentation for {DatastaxRails::SearchMethods} for more examples of using the search API.
  class Base
    include ActiveModel::Model
    extend ActiveSupport::DescendantsTracker
    
    include Persistence
    include Connection
    include Inheritance
    include FinderMethods
    include Batches
    include AttributeAssignment
    include AttributeMethods
    include Validations
    include Callbacks
    include Reflection
    include Associations
    include Scoping
    include Timestamps
    include Serialization
    include SolrRepair
    
    # Stores the default scope for the class
    class_attribute :default_scopes, :instance_writer => false
    self.default_scopes = []
    
    # Stores the connection configuration information
    class_attribute :config
    
    class_attribute :default_timezone, :instance_writer => false
    self.default_timezone = :utc

    # Stores the default consistency level (QUORUM by default)
    class_attribute :default_consistency
    self.default_consistency = :quorum

    # Stores the method of saving data (CQL by default)
    class_attribute :storage_method
    self.storage_method = :cql
    
    # Stores any additional information that should be used when creating the column family
    # See {DatastaxRails::WideStorageModel} or {DatastaxRails::Payload} model for an example
    class_attribute :create_options
    
    # Stores the attribute that wide models should cluster on. Basically, this is the
    # attribute that CQL uses to "group" columns into logical records even though they
    # are stored on the same row.
    class_attribute :cluster_by
    
    attr_reader :attributes
    attr_reader :loaded_attributes
    attr_accessor :key
    
    # Returns a hash of all the attributes that have been specified for serialization as
    # keys and their class restriction as values.
    class_attribute :serialized_attributes
    self.serialized_attributes = {}
    
    # Whether or not we are using solr legacy mappings
    class_attribute :legacy_mapping
    
    def initialize(attributes = {}, options = {})
      defaults = self.class.column_defaults.dup
      defaults.each { |k, v| v.duplicable? ? v.dup : v }
      
      @attributes = self.initialize_attributes(defaults)
      @column_types = self.class.columns_hash

      init_internals
      init_changed_attributes      
      populate_with_current_scope_attributes
      
      assign_attributes(attributes) if attributes
      
      yield self if block_given?
      run_callbacks :initialize unless _initialize_callbacks.empty?
    end
    
    # Initialize an empty model object from +coder+. +coder+ must contain
    # the attributes necessary for initializing an empty model object. For
    # example:
    #
    #   class Post < DatastaxRails::Base
    #   end
    #
    #   post = Post.allocate
    #   post.init_with('attributes' => { 'title' => 'hello world' })
    #   post.title # => 'hello world'
    def init_with(coder)
      Types::DirtyCollection.ignore_modifications do
        @attributes   = self.initialize_attributes(coder['attributes'])
        @column_types_override = coder['column_types']
        @column_types = self.class.columns_hash
        
        init_internals
  
        @new_record = false
        run_callbacks :find
        run_callbacks :initialize
      end
      self
    end
    
    def init_internals
      pk = self.class.primary_key
      @attributes[pk] = nil unless @attributes.key?(pk)

      @association_cache = {}
      @attributes_cache = {}
      @previously_changed = {}.with_indifferent_access
      @changed_attributes = {}.with_indifferent_access
      @loaded_attributes = Hash[@attributes.map{|k,v| [k,true]}].with_indifferent_access
      @readonly = false
      @destroyed = false
      @marked_for_destruction = false
      @destroyed_by_association = nil
      @new_record = true
    end
    
    def init_changed_attributes
      # Intentionally avoid using #column_defaults since overridden defaults
      # won't get written unless they get marked as changed
      self.class.columns.each do |c|
        attr, orig_value = c.name, c.default
        @changed_attributes[attr] = orig_value if _field_changed?(attr, orig_value, @attributes[attr])
      end
    end
    
    # Freeze the attributes hash such that associations are still accessible, even on destroyed records.
    def freeze
      @attributes.freeze; self
    end

    # Returns +true+ if the attributes hash has been frozen.
    def frozen?
      @attributes.frozen?
    end
    
    def to_param
      id.to_s if persisted?
    end

    def hash
      id.hash
    end

    def ==(comparison_object)
      super ||
        comparison_object.instance_of?(self.class) &&
        id.present? &&
        comparison_object.id.eql?(id)
    end

    def eql?(comparison_object)
      self == (comparison_object)
    end
    
    def attribute_names
      self.class.attribute_names
    end
    alias :column_names :attribute_names
    
    def valid_consistency?(level) #:nodoc:
      self.class.validate_consistency(level.to_s.upcase)
    end
    
    private
      def populate_with_current_scope_attributes
        return unless self.class.scope_attributes?
        
        self.class.scope_attributes.each do |att, value|
          send("#{att}=", value) if respond_to?("#{att}=")
        end
      end
      
    class << self
      delegate :find, :find_by, :find_by!, :first, :all, :exists?, :any?, :many?, :to => :scoped
      delegate :destroy, :destroy_all, :delete, :update, :update_all, :to => :scoped
      delegate :order, :limit, :where, :where_not, :page, :paginate, :select, :slow_order, :to => :scoped
      delegate :per_page, :each, :group, :total_pages, :search, :fulltext, :to => :scoped
      delegate :count, :first, :first!, :last, :last!, :compute_stats, :to => :scoped
      delegate :sum, :average, :minimum, :maximum, :stddev, :to => :scoped
      delegate :cql, :with_cassandra, :with_solr, :commit_solr, :allow_filtering, :to => :scoped
      delegate :find_each, :find_in_batches, :consistency, :to => :scoped
      delegate :field_facet, :range_facet, :to => :scoped

      # Sets the column family name
      #
      # @param [String] column_family the name of the column family in cassandra
      def column_family=(column_family)
        @column_family = column_family
      end

      # Returns the column family name.  If it has been set manually, the set name is returned.
      # Otherwise returns the pluralized version of the class name.
      #
      # Returns [String] the name of the column family
      def column_family
        @column_family || name.underscore.pluralize
      end
      
      def models
        self.descendants.reject {|m|m.abstract_class?}
      end
      
      def payload_model?
        self.ancestors.include?(DatastaxRails::PayloadModel)
      end
      
      def wide_storage_model?
        self.ancestors.include?(DatastaxRails::WideStorageModel)
      end
      
      def legacy_mapping?
        self.legacy_mapping
      end
      
      def base_class
        klass = self
        while klass.superclass != Base
          klass = klass.superclass
        end
        klass
      end
      
      def find_by_id(id)
        scoped.with_cassandra.find(id)
      rescue RecordNotFound
        nil
      end
      
      def logger
        Rails.logger
      end
      
      # Returns an array of attribute names as strings
      def attribute_names
        @attribute_names ||= attribute_definitions.keys.collect {|a|a.to_s}
      end
      alias :column_names :attribute_names
      
      def columns
        @columns ||= attribute_definitions.values
      end
      
      # SOLR always paginates all requests.  There is no way to disable it, so we are
      # setting the default page size to an arbitrarily high number so that we effectively
      # remove pagination.  If you instead want a model set to something more sane, then
      # override this method in your model and set it.  Of course, the page size can
      # always be raised or lowered for an individual request.
      #
      #   class Model < DatastaxRails::Base
      #     def self.default_page_size
      #       30
      #     end
      #   end
      def default_page_size
        100000
      end
      
      def search_ids(&block)
        search = solr_search(&block)
        search.raw_results.map { |result| result.primary_key }
      end
      
      def valid_consistency?(level) #:nodoc:
        DatastaxRails::Cql::Consistency::VALID_CONSISTENCY_LEVELS.include?(level)
      end
      
      # Returns a string like 'Post(id:integer, title:string, body:text)'
      def inspect
        if self == Base
          super
        else
          attr_list = columns.map { |c| "#{c.name}: #{c.type}" } * ', '
          "#{super}(#{attr_list})"
        end
      end
      
      private
      
        def construct_finder_relation(options = {}, scope = nil)
          relation = options.is_a?(Hash) ? unscoped.apply_finder_options(options) : options
          relation = scope.merge(relation) if scope
          relation
        end
      
        def relation #:nodoc:
          Relation.new(self, column_family)
        end
        
        # Returns the class type of the record using the current module as a prefix. So descendants of
        # MyApp::Business::Account would appear as MyApp::Business::AccountSubclass.
        def compute_type(type_name)
          if type_name.match(/^::/)
            # If the type is prefixed with a scope operator then we assume that
            # the type_name is an absolute reference.
            ActiveSupport::Dependencies.constantize(type_name)
          else
            # Build a list of candidates to search for
            candidates = []
            name.scan(/::|$/) { candidates.unshift "#{$`}::#{type_name}" }
            candidates << type_name

            candidates.each do |candidate|
              begin
                constant = ActiveSupport::Dependencies.constantize(candidate)
                return constant if candidate == constant.to_s
              rescue NameError => e
                # We don't want to swallow NoMethodError < NameError errors
                raise e unless e.instance_of?(NameError)
              end
            end

            raise NameError, "uninitialized constant #{candidates.first}"
          end
        end
    end
  end
end
