require 'rsolr'

module DatastaxRails
  class Relation
    MULTI_VALUE_METHODS = [:group, :order, :where, :where_not, :fulltext, :greater_than, :less_than, :select]
    SINGLE_VALUE_METHODS = [:page, :per_page, :reverse_order, :query_parser, :consistency, :ttl, :use_solr]
    
    Relation::MULTI_VALUE_METHODS.each do |m|
      attr_accessor :"#{m}_values"
    end
    Relation::SINGLE_VALUE_METHODS.each do |m|
      attr_accessor :"#{m}_value"
    end
    attr_accessor :create_with_value, :default_scoped
    
    include SearchMethods
    include ModificationMethods
    include FinderMethods
    include SpawnMethods
    
    attr_reader :klass, :column_family, :loaded, :cql
    alias :loaded? :loaded
    alias :default_scoped? :default_scoped
    
    def initialize(klass, column_family) #:nodoc:
      @klass, @column_family = klass, column_family
      @loaded = false
      @results = []
      @default_scoped = false
      @cql = DatastaxRails::Cql.for_class(klass)
      
      SINGLE_VALUE_METHODS.each {|v| instance_variable_set(:"@#{v}_value", nil)}
      MULTI_VALUE_METHODS.each {|v| instance_variable_set(:"@#{v}_values", [])}
      @per_page_value = @klass.default_page_size
      @page_value = 1
      @use_solr_value = true
      @consistency_value = "QUORUM"
      @extensions = []
      @create_with_value = {}
      apply_default_scope
    end
    
    # Returns true if the two relations have the same query parameters
    def ==(other)
      case other
      when Relation
        # This is not a valid implementation.  It's a placeholder till I figure out the right way.
        MULTI_VALUE_METHODS.each do |m|
          return false unless other.send("#{m}_values") == self.send("#{m}_values")
        end
        SINGLE_VALUE_METHODS.each do |m|
          return false unless other.send("#{m}_value") == self.send("#{m}_value")
        end
        return true
      when Array
        to_a == other
      end
    end
    
    # Returns true if there are any results given the current criteria
    def any?
      if block_given?
        to_a.any? { |*block_args| yield(*block_args) }
      else
        !empty?
      end
    end
    alias :exists? :any?
    
    # Returns the total number of entries that match the given search.
    # This means the total number of matches regardless of page size.
    # Compare with #size.
    def count
      @count ||= self.use_solr_value ? count_via_solr : count_via_cql
    end
    
    # Returns the current page for will_paginate compatibility
    def current_page
      self.page_value.try(:to_i)
    end
    
    # current_page - 1 or nil if there is no previous page
    def previous_page
      current_page > 1 ? (current_page - 1) : nil
    end

    # current_page + 1 or nil if there is no next page
    def next_page
      current_page < total_pages ? (current_page + 1) : nil
    end
    
    # Gets a default scope with no conditions or search attributes set.
    def default_scope
      clone.tap do |r|
        SINGLE_VALUE_METHODS.each {|v| r.instance_variable_set(:"@#{v}_value", nil)}
        MULTI_VALUE_METHODS.each {|v| r.instance_variable_set(:"@#{v}_values", [])}
        apply_default_scope
      end
    end
    
    # Returns true if there are no results given the current criteria
    def empty?
      return @results.empty? if loaded?
      
      c = count
      c.respond_to?(:zero?) ? c.zero? : c.empty?
    end
    
    # Returns true if there are multiple results given the current criteria
    def many?
      if block_given?
        to_a.many? { |*block_args| yield(*block_args) }
      else
        count > 1
      end
    end
    
    # Constructs a new instance of the class this relation points to
    def new(*args, &block)
      scoping { @klass.new(*args, &block) }
    end
    
    # Reloads the results from Solr
    def reload
      reset
      to_a
      self
    end
    
    # Empties out the current results.  The next call to to_a
    # will re-run the query.
    def reset
      @loaded = @first = @last = @scope_for_create = @count = nil
      @results = []
    end
    
    def initialize_copy(other) #:nodoc:
      reset
      @search = nil
    end
    
    def clone #:nodoc:
      dup.tap do |r|
        MULTI_VALUE_METHODS.each do |m|
          r.send("#{m}_values=", Marshal.load(Marshal.dump(self.send("#{m}_values"))))
        end
        SINGLE_VALUE_METHODS.each do |m|
          r.send("#{m}_value=", Marshal.load(Marshal.dump(self.send("#{m}_value")))) if self.send("#{m}_value")
        end
      end
    end
    
    # Returns the size of the total result set for the given criteria
    # NOTE that this takes pagination into account so will only return
    # the number of results in the current page.  DatastaxRails models
    # can have a +default_page_size+ set which will cause them to be
    # paginated all the time.
    # Compare with #count
    def size
      return @results.size if loaded?
      total_entries = count
      (per_page_value && total_entries > per_page_value) ? per_page_value : total_entries
    end
    
    # Returns the total number of pages required to display the results
    # given the current page size.  Used by will_paginate.
    def total_pages
      return 1 unless @per_page_value
      (count / @per_page_value.to_f).ceil
    end
    
    # Actually executes the query if not already executed.
    # Returns a standard array thus no more methods may be chained.
    def to_a
      return @results if loaded?
      if use_solr_value
        @results = query_via_solr
        @count = @results.total_entries
      else
        @results = query_via_cql
      end
      @loaded = true
      @results
    end
    alias :all :to_a
    alias :results :to_a
    
    def create(*args, &block)
      scoping { @klass.create(*args, &block) }
    end

    def create!(*args, &block)
      scoping { @klass.create!(*args, &block) }
    end
    
    def respond_to?(method, include_private = false) #:nodoc:
        Array.method_defined?(method)                       ||
        @klass.respond_to?(method, include_private)         ||
        super
    end
    
    def count_via_cql
      # Counting via CQL does not work
      with_solr.count_via_solr
    end
    
    def query_via_cql
      cql = @cql.select(@klass.attribute_definitions.keys - @klass.lazy_attributes)
      cql.using(@consistency_value) if @consistency_value
      @where_values.each do |wv|
        cql.conditions(wv)
      end
      results = []
      CassandraCQL::Result.new(cql.execute).fetch do |row|
        results << @klass.instantiate(row.row.key,row.to_hash)
      end
      results
    end
    
    def count_via_solr
      limit(1).to_a.total_entries
    end
    
    def query_via_solr
      filter_queries = []
      orders = []
      @where_values.each do |wv|
        wv.each do |k,v|
          # If v is blank, check that there is no value for the field in the document
          filter_queries << (v.blank? ? "-#{k}:[* TO *]" : "#{k}:(#{v})")
        end
      end
      
      @where_not_values.each do |wnv|
        wnv.each do |k,v|
          # If v is blank, check for any value for the field in document
          filter_queries << (v.blank? ? "#{k}:[* TO *]" : "-#{k}:(#{v})")
        end
      end
      
      @greater_than_values.each do |gtv|
        gtv.each do |k,v|
          filter_queries << "#{k}:[#{v} TO *]"
        end
      end
      
      @less_than_values.each do |ltv|
        ltv.each do |k,v|
          filter_queries << "#{k}:[* TO #{v}]"
        end
      end
      
      @order_values.each do |ov|
        ov.each do |k,v|
          if(@reverse_order_value)
            orders << "#{k} #{v == :asc ? 'desc' : 'asc'}"
          else
            orders << "#{k} #{v == :asc ? 'asc' : 'desc'}"
          end
        end
      end
      
      sort = orders.join(",")
      
      if @fulltext_values.empty?
        q = "*:*"
      else
        q = @fulltext_values.collect {|ftv| "(" + ftv[:query] + ")"}.join(' AND ')
      end
      
      #TODO highlighting and fielded queries of fulltext
      
      params = {:q => q}
      unless sort.empty?
        params[:sort] = sort
      end
      
      unless filter_queries.empty?
        params[:fq] = filter_queries
      end
      
      #TODO Need to escape URL stuff (I think)
      response = rsolr.paginate(@page_value, @per_page_value, 'select', :params => params)["response"]
      results = DatastaxRails::Collection.new
      results.total_entries = response['numFound'].to_i
      response['docs'].each do |doc|
        key = doc.delete('id')
        results << @klass.instantiate(key,doc)
      end
      results
    end
    
    def inspect(just_me = false)
      just_me ? super() : to_a.inspect
    end
    
    # Scope all queries to the current scope.
    #
    # ==== Example
    #
    #   Comment.where(:post_id => 1).scoping do
    #     Comment.first # SELECT * FROM comments WHERE post_id = 1
    #   end
    #
    # Please check unscoped if you want to remove all previous scopes (including
    # the default_scope) during the execution of a block.
    def scoping
      @klass.send(:with_scope, self, :overwrite) { yield }
    end
    
    def where_values_hash
      where_values.inject({}) { |values,v| values.merge(v) }
    end

    def scope_for_create
      @scope_for_create ||= where_values_hash.merge(create_with_value)
    end
    
    # def scoped #:nodoc:
      # self
    # end
    
    def commit_solr
      rsolr.commit :commit_attributes => {}
    end
    
    # Everything that gets indexed into solr is downcased as part of the analysis phase.
    # Normally, this is done to the query as well, but if your query includes wildcards
    # then analysis isn't performed.  This means that the query does not get downcased.
    # We therefore need to perform the downcasing ourselves.  This does it while still
    # leaving boolean operations (AND, OR, NOT) upcased.
    def self.downcase_query(value)
      if(value.is_a?(String))
        value.split(/\bAND\b/).collect do |a|
          a.split(/\bOR\b/).collect do |o| 
            o.split(/\bNOT\b/).collect do |n| 
              n.downcase
            end.join("NOT")
          end.join("OR")
        end.join("AND")
      else
        value
      end
    end
    
    protected
      
      def method_missing(method, *args, &block)
        if Array.method_defined?(method)
          to_a.send(method, *args, &block)
        elsif @klass.respond_to?(method)
          scoping { @klass.send(method, *args, &block) }
        else
          super
        end
      end
      
      def rsolr
        @rsolr ||= RSolr.connect :url => "#{DatastaxRails::Base.config[:solr][:url]}/#{DatastaxRails::Base.connection.keyspace}.#{@klass.column_family}"
      end
  end
end