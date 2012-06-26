require 'active_record/dynamic_finder_match'
require 'active_record/dynamic_scope_match'
require 'datastax_rails/log_subscriber'
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
    include ActiveModel::MassAssignmentSecurity
    
    include Connection
    include Consistency
    include Identity
    include FinderMethods
    include Batches
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
    include Migrations
    # include Mocking
    
    # Stores the default scope for the class
    class_attribute :default_scopes, :instance_writer => false
    self.default_scopes = []
    
    # Stores the configuration information
    class_attribute :config
    
    class_attribute :models
    self.models = []
    
    attr_reader :attributes
    attr_accessor :key
    
    def initialize(attributes = {}, options = {})
      @key = attributes.delete(:key)
      @attributes = {}
      
      @relation = nil
      @new_record = true
      @destroyed = false
      @previously_changed = {}
      @changed_attributes = {}
      @schema_version = self.class.current_schema_version
      
      populate_with_current_scope_attributes
      
      sanitize_for_mass_assignment(attributes).each do |k,v|
        if respond_to?("#{k.to_s.downcase}=")
          send("#{k.to_s.downcase}=",v)
        else
          raise(UnknownAttributeError, "unknown attribute: #{k}")
        end
      end
      
      yield self if block_given?
      run_callbacks :initialize
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
    
    # Allows you to set all the attributes for a particular mass-assignment
    # security role by passing in a hash of attributes with keys matching
    # the attribute names (which again matches the column names) and the role
    # name using the :as option.
    #
    # To bypass mass-assignment security you can use the :without_protection => true
    # option.
    #
    #   class User < ActiveRecord::Base
    #     attr_accessible :name
    #     attr_accessible :name, :is_admin, :as => :admin
    #   end
    #
    #   user = User.new
    #   user.assign_attributes({ :name => 'Josh', :is_admin => true })
    #   user.name # => "Josh"
    #   user.is_admin? # => false
    #
    #   user = User.new
    #   user.assign_attributes({ :name => 'Josh', :is_admin => true }, :as => :admin)
    #   user.name # => "Josh"
    #   user.is_admin? # => true
    #
    #   user = User.new
    #   user.assign_attributes({ :name => 'Josh', :is_admin => true }, :without_protection => true)
    #   user.name # => "Josh"
    #   user.is_admin? # => true
    def assign_attributes(new_attributes, options = {})
      return unless new_attributes

      attributes = new_attributes.stringify_keys
      multi_parameter_attributes = []
      @mass_assignment_options = options

      unless options[:without_protection]
        attributes = sanitize_for_mass_assignment(attributes, mass_assignment_role)
      end

      attributes.each do |k, v|
        if respond_to?("#{k.to_s.downcase}=")
          send("#{k.to_s.downcase}=",v)
        else
          raise(UnknownAttributeError, "unknown attribute: #{k}")
        end
      end

      @mass_assignment_options = nil
    end
    
    def attribute_names
      self.class.attribute_names
    end
    
    private
      def populate_with_current_scope_attributes
        return unless self.class.scope_attributes?
        
        self.class.scope_attributes.each do |att, value|
          send("#{att}=", value) if respond_to?("#{att}=")
        end
      end
      
    class << self
      delegate :find, :first, :all, :exists?, :any?, :many?, :to => :scoped
      delegate :destroy, :destroy_all, :delete, :delete_all, :update, :update_all, :to => :scoped
      # delegate :find_each, :find_in_batches, :to => :scoped
      delegate :order, :limit, :where, :where_not, :page, :paginate, :to => :scoped
      delegate :per_page, :each, :group, :total_pages, :search, :fulltext, :to => :scoped
      delegate :count, :first, :first!, :last, :last!, :to => :scoped
      delegate :cql, :with_cassandra, :with_solr, :commit_solr, :to => :scoped

      def column_family=(column_family)
        @column_family = column_family
      end

      def column_family
        @column_family || name.pluralize
      end

      def base_class
        klass = self
        while klass.superclass != Base
          klass = klass.superclass
        end
        klass
      end
      
      # def find(*keys)
        # scoped.with_cassandra.find(keys)
      # end
      
      def logger
        Rails.logger
      end
      
      def respond_to?(method_id, include_private = false)
        if match = ActiveRecord::DynamicFinderMatch.match(method_id)
          return true if all_attributes_exists?(match.attribute_names)
        elsif match = ActiveRecord::DynamicScopeMatch.match(method_id)
          return true if all_attributes_exists?(match.attribute_names)
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
      
      protected
      
        
      
      private
      
        def construct_finder_relation(options = {}, scope = nil)
          relation = options.is_a(Hash) ? unscoped.apply_finder_options(options) : options
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
          if match = ActiveRecord::DynamicFinderMatch.match(method_id)
            attribute_names = match.attribute_names
            super unless all_attributes_exist?(attribute_names)
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
            super unless all_attributes_exist?(attribute_names)
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
        end
        
        def all_attributes_exist?(attribute_names)
          (attribute_names - self.attribute_names).empty?
        end
        
        def relation #:nodoc:
          @relation ||= Relation.new(self, column_family)
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
