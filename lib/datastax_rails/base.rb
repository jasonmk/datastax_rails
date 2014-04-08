if Rails.version =~ /^3.*/
  # Dynamic finders are only supported in Rails 3.x applications (depricated in 4.x)
  require 'active_record/dynamic_finder_match'
  require 'active_record/dynamic_scope_match'
elsif Rails.version =~ /^4.*./
  require 'active_record/deprecated_finders/dynamic_matchers'
end
require 'datastax_rails/types'
require 'datastax_rails/errors'
module DatastaxRails #:nodoc:
  # = DatastaxRails
  #
  # DatastaxRails-based objects differ from Active Record objects in that they specify their
  # attributes directly on the model.  This is necessary because of the fact that Cassandra
  # column families do not have a set list of columns but rather can have different columns per
  # row.  By specifying the attributes on the model, getters and setters are automatically
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
  #     key :uuid
  #   end
  #
  # If you want to use a natural key (i.e., one or more of the columns of your data),
  # the following would work.
  #
  #   class Person < DatastaxRails::Base
  #     key :natural, :attributes => [:last_name, :first_name]
  #   end
  #
  # Finally, you can create a custom key based on a method on your model.
  #
  #   class Person < DatastaxRails::Base
  #     key :custom, :method => :my_key
  #     
  #     def my_key
  #       # Some logic to generate a key
  #     end
  #   end
  #
  # == Attributes
  #
  # Attributes are specified near the top of the model. The following attribute types
  # are supported:
  #
  # * array - an array of strings
  # * binary - a large object that will not be indexed into SOLR (e.g., BLOB)
  # * boolean - true/false values
  # * date - a date without a time component
  # * float - a number in floating point notation
  # * integer - a whole, round number of any size
  # * string - a generic string type that is not tokenized by default
  # * text - like strings but will be tokenized for full-text searching by default
  # * time - a datetime object
  # * timestamps - a special type that instructs DSR to include created_at and updated_at
  #
  # The following options may be specified on the various types to control how they
  # are indexed into SOLR:
  #
  # * indexed - If the attribute should the attribute be indexed into SOLR.
  #   Defaults to true for everything but binary.
  # * stored - If the attribute should the attribute be stored in SOLR.
  #   Defaults to true for everything but binary. (see note)
  # * sortable - If the attribute should be sortable by SOLR.
  #   Defaults to true for everything but binary and text. (see note)
  # * tokenized - If the attribute should be tokenized for full-text searching within the field.
  #   Defaults to true for array and text. (see note)
  # * fulltext - If the attribute should be included in the default field for full-text searches.
  #   Defaults to true for text and string.
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
  # * Arrays are tokenized specially. Each element of the array is treated as a single token.
  #   This means that you can match against any single element, but you cannot search within
  #   elements. This functionality may be added at a later time.
  #
  # EXAMPLE:
  # 
  #   class Person < DatastaxRails::Base
  #     key     :uuid
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
  # Cassandra itself is a 'schema-optional' database.  In general, DSR does not make use of
  # Cassandra schemas.  SOLR on the other hand does use a schema to define the data and how
  # it should be indexed.  There is a rake task to upload the latest SOLR schema based on
  # the model files.  When this happens, if the column family does not exist yet, it will be
  # created.  Therefore, migrations to create column families are unnecessary.  If the
  # column family does exist, and the new schema differs, the columns that are changed will
  # be automatically reindexed.
  #
  # TODO: Need a way to remove ununsed column families.
  #
  # == Creation
  #
  # DatastaxRails objects accept constructor parameters either in a hash or as a block. The hash
  # method is especially useful when you're receiving the data from somewhere else, like an
  # HTTP request. It works like this:
  #
  #   user = User.new(:name => "David", :occupation => "Code Artist")
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
  # http://www.datastax.com/docs/1.0/dml/data_consistency
  #
  # DatastaxRails allows you to specify the consistency when you save and retrieve objects.
  #
  #   user = User.new(:name => 'David')
  #   user.save(:consistency => 'ALL')
  #
  #   User.create(params[:user], {:consistency => :local_quorum})
  #
  #   User.consistency(:local_quorum).where(:name => 'David')
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
  #   Student.where(:first_name => "Harvey", :status => 1)
  #   Student.where(params[:student])
  #
  # A range may be used in the hash to use a SOLR range query:
  #
  #   Student.where(:grade => 9..12)
  #
  # An array may be used in the hash to construct a SOLR OR query:
  #
  #   Student.where(:grade => [9,11,12])
  #
  # Inequality can be tested for like so:
  #
  #   Student.where_not(:grade => 9)
  #   Student.where(:grade).greater_than(9)
  #   Student.where(:grade).less_than(10)
  #
  # Fulltext searching is natively supported.  All text fields are automatically indexed for fulltext
  # searching.
  #
  #   Post.fulltext('Apple AND "iPhone 4s"')
  #
  # See the documentation on DatastaxRails::SearchMethods for more information and examples.
  #
  # == Overwriting default accessors
  #
  # All column values are automatically available through basic accessors on the DatastaxRails,
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
  # Note: These are only available in Rails 3.x applications, and are not supported in Rails 4.x
  #
  # Dynamic attribute-based finders are a cleaner way of getting (and/or creating) objects
  # by simple queries without using where chains. They work by appending the name of an attribute
  # to <tt>find_by_</tt> or <tt>find_all_by_</tt> and thus produces finders
  # like <tt>Person.find_by_user_name</tt>, <tt>Person.find_all_by_last_name</tt>, and
  # <tt>Payment.find_by_transaction_id</tt>. Instead of writing
  # <tt>Person.where(:user_name => user_name).first</tt>, you just do <tt>Person.find_by_user_name(user_name)</tt>.
  # And instead of writing <tt>Person.where(:last_name => last_name).all</tt>, you just do
  # <tt>Person.find_all_by_last_name(last_name)</tt>.
  #
  # It's also possible to use multiple attributes in the same find by separating them with "_and_".
  #
  #   Person.where(:user_name => user_name, :password => password).first
  #   Person.find_by_user_name_and_password(user_name, password) # with dynamic finder
  #
  # It's even possible to call these dynamic finder methods on relations and named scopes.
  #
  #   Payment.order("created_on").find_all_by_amount(50)
  #   Payment.pending.find_last_by_amount(100)
  #
  # The same dynamic finder style can be used to create the object if it doesn't already exist.
  # This dynamic finder is called with <tt>find_or_create_by_</tt> and will return the object if
  # it already exists and otherwise creates it, then returns it. Protected attributes won't be set
  # unless they are given in a block.
  #
  # NOTE: This functionality is currently unimplemented but will be in a release in the near future.
  #
  #   # No 'Summer' tag exists
  #   Tag.find_or_create_by_name("Summer") # equal to Tag.create(:name => "Summer")
  #
  #   # Now the 'Summer' tag does exist
  #   Tag.find_or_create_by_name("Summer") # equal to Tag.find_by_name("Summer")
  #
  #   # Now 'Bob' exist and is an 'admin'
  #   User.find_or_create_by_name('Bob', :age => 40) { |u| u.admin = true }
  #
  # Use the <tt>find_or_initialize_by_</tt> finder if you want to return a new record without
  # saving it first. Protected attributes won't be set unless they are given in a block.
  #
  #   # No 'Winter' tag exists
  #   winter = Tag.find_or_initialize_by_name("Winter")
  #   winter.persisted? # false
  #
  # Just like <tt>find_by_*</tt>, you can also use <tt>scoped_by_*</tt> to retrieve data. The good thing about
  # using this feature is that the very first time result is returned using <tt>method_missing</tt> technique
  # but after that the method is declared on the class. Henceforth <tt>method_missing</tt> will not be hit.
  #
  #   User.scoped_by_user_name('David')
  #
  # == Facets
  #
  # DSR support both field and range facets.  For additional detail on facets, see the documentation
  # available under the FacetMethods module.  The result is available through the facets accessor
  #
  # Facet examples:
  #
  # results = Article.field_facet(:author)
  # results.facets => {"author"=>["vonnegut", 2. "asimov", 3]} 
  #
  # Model.field_facet(:author)
  # Model.field_facet(:author, :sort => 'count', :limit => 10, :mincount => 1)
  # Model.range_facet(:price, 500, 1000, 10)
  # Model.range_facet(:price, 500, 1000, 10, :include => 'all')
  # Model.range_facet(:publication_date, "1968-01-01T00:00:00Z", "2000-01-01T00:00:00Z", "+1YEAR")
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
  # * MultiparameterAssignmentErrors - Collection of errors that occurred during a mass assignment using the
  #   <tt>attributes=</tt> method. The +errors+ property of this exception contains an array of
  #   AttributeAssignmentError objects that should be inspected to determine which attributes triggered the errors.
  # * AttributeAssignmentError - An error occurred while doing a mass assignment through the
  #   <tt>attributes=</tt> method.
  #   You can inspect the +attribute+ property of the exception object to determine which attribute
  #   triggered the error.
  #
  # See the documentation for SearchMethods for more examples of using the search API.
  class Base
    extend ActiveModel::Naming
    include ActiveModel::Conversion
    extend ActiveSupport::DescendantsTracker
    
    include Connection
    include Inheritance
    include Identity
    include FinderMethods
    include Batches
    include AttributeAssignment
    include AttributeMethods
    include AttributeMethods::Dirty
    include AttributeMethods::Typecasting
    include Persistence
    include Callbacks
    include Validations
    include Reflection
    include Associations
    include Scoping
    include Timestamps
    include Serialization
    include SolrRepair
    
    # Stores the default scope for the class
    class_attribute :default_scopes, :instance_writer => false
    self.default_scopes = []
    
    # Stores the configuration information
    class_attribute :config
    
    class_attribute :default_consistency
    self.default_consistency = :quorum

    class_attribute :storage_method
    self.storage_method = :cql
    
    class_attribute :primary_key_name
    self.primary_key_name = 'key'
    
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
      @key = parse_key(attributes.delete(:key))
      @attributes = {}.with_indifferent_access
      @association_cache = {}
      @loaded_attributes = {}.with_indifferent_access
      
      @new_record = true
      @destroyed = false
      @previously_changed = {}
      @changed_attributes = {}
      
      __set_defaults
      
      populate_with_current_scope_attributes
      
      assign_attributes(attributes, options) if attributes
      
      yield self if block_given?
      run_callbacks :initialize
    end
    
    # Set any default attributes specified by the schema
    def __set_defaults
      self.class.attribute_definitions.each do |a,d|
        unless(d.coder.default.nil?)
          self.attributes[a]=d.coder.default
          self.send(a.to_s+"_will_change!")
        end
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
      comparison_object.equal?(self) ||
        (comparison_object.instance_of?(self.class) &&
          comparison_object.key == key &&
          !comparison_object.new_record?)
    end

    def eql?(comparison_object)
      self == (comparison_object)
    end
    
    def attribute_names
      self.class.attribute_names
    end
    
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
      
      def respond_to?(method_id, include_private = false)
       
        if Rails.version =~ /^3.*/
          if match = ActiveRecord::DynamicFinderMatch.match(method_id)
            return true if all_attributes_exists?(match.attribute_names)
          elsif match = ActiveRecord::DynamicScopeMatch.match(method_id)
            return true if all_attributes_exists?(match.attribute_names)
          end
        elsif Rails.version =~ /^4.*/
          if match = ActiveRecord::DynamicMatchers::Method.match(self, method_id)
            return true if all_attributes_exists?(match.attribute_names)
          end
        end
        
        super
      end
      
      # Returns an array of attribute names as strings
      def attribute_names
        @attribute_names ||= attribute_definitions.keys.collect {|a|a.to_s}
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
      
      protected
      
        
      
      private
      
        def construct_finder_relation(options = {}, scope = nil)
          relation = options.is_a?(Hash) ? unscoped.apply_finder_options(options) : options
          relation = scope.merge(relation) if scope
          relation
        end
      
        # Enables dynamic finders like <tt>User.find_by_user_name(user_name)</tt> and
        # <tt>User.scoped_by_user_name(user_name).
        #
        # It's even possible to use all the additional parameters to +find+. For example, the
        # full interface for +find_all_by_amount+ is actually <tt>find_all_by_amount(amount, options)</tt>.
        #
        # Each dynamic finder using <tt>scoped_by_*</tt> is also defined in the class after it
        # is first invoked, so that future attempts to use it do not run through method_missing.
        def method_missing(method_id, *arguments, &block)
          if Rails.version =~ /^3.*/
            if match = ActiveRecord::DynamicFinderMatch.match(method_id)
              attribute_names = match.attribute_names
              super unless all_attributes_exists?(attribute_names)
              if !arguments.first.is_a?(Hash) && arguments.size < attribute_names.size
                ActiveSupport::Deprecation.warn(
                  "Calling dynamic finder with less number of arguments than the number of attributes in " \
                  "method name is deprecated and will raise an ArguementError in the next version of Rails. " \
                  "Please passing `nil' to the argument you want it to be nil."
                )
              end
              if match.finder?
                options = arguments.extract_options!
                relation = options.any? ? scoped(options) : scoped
                relation.send :find_by_attributes, match, attribute_names, *arguments
              elsif match.instantiator?
                scoped.send :find_or_instantiator_by_attributes, match, attribute_names, *arguments, &block
              end
            elsif match = ActiveRecord::DynamicScopeMatch.match(method_id)
              attribute_names = match.attribute_names
              super unless all_attributes_exists?(attribute_names)
              if arguments.size < attribute_names.size
                ActiveSupport::Deprecation.warn(
                  "Calling dynamic scope with less number of arguments than the number of attributes in " \
                  "method name is deprecated and will raise an ArguementError in the next version of Rails. " \
                  "Please passing `nil' to the argument you want it to be nil."
                )
              end
              if match.scope?
                self.class_eval <<-METHOD, __FILE__, __LINE__ + 1
                  def self.#{method_id}(*args)                                    # def self.scoped_by_user_name_and_password(*args)
                    attributes = Hash[[:#{attribute_names.join(',:')}].zip(args)] #   attributes = Hash[[:user_name, :password].zip(args)]
                    scoped(:conditions => attributes)                             #   scoped(:conditions => attributes)
                  end                                                             # end
                  METHOD
                send(method_id, *arguments)
              end
            else
              super
            end
          elsif Rails.version =~ /^4.*/
            if match = ActiveRecord::DynamicMatchers::Method.match(self, method_id)
              attribute_names = match.attribute_names
              super unless all_attributes_exists?(attribute_names)
              if !arguments.first.is_a?(Hash) && arguments.size < attribute_names.size
                ActiveSupport::Deprecation.warn(
                  "Calling dynamic scope with less number of arguments than the number of attributes in " \
                  "method name is deprecated and will raise an ArguementError in the next version of Rails. " \
                  "Please passing `nil' to the argument you want it to be nil."
                )
              end
              if match.finder.present?
                options = arguments.extract_options!
                relation = options.any? ? scoped(options) : scoped
                relation.send :find_by_attributes, match, attribute_names, *arguments
              elsif match.instantiator?
                scoped.send :find_or_instantiator_by_attributes, match, attribute_names, *arguments, &block
              end
            end
          else
            super
          end
        end
        
        def all_attributes_exists?(attribute_names)
          (attribute_names - self.attribute_names).empty?
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
