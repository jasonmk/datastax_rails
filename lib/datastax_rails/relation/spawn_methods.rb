module DatastaxRails
  # Relation methods for building a new relation from an existing one (which may be empty).
  module SpawnMethods
    def merge(r) #:nodoc:
      return self unless r
      return to_a & r if r.is_a?(Array)

      merged_relation = clone

      r = r.with_default_scope if r.default_scoped? && r.klass != klass

      (Relation::MULTI_VALUE_METHODS - [:where, :where_not]).each do |method|
        value = r.send(:"#{method}_values")
        merged_relation.send(:"#{method}_values=", merged_relation.send(:"#{method}_values") + value) if value.present?
      end

      merged_wheres = {}
      # This will merge all the where clauses into a single hash.  If the same attribute is
      # specified multiple times, the last one will win.
      (@where_values + r.where_values).each { |w| merged_wheres.merge!(w) }

      merged_relation.where_values = [merged_wheres] unless merged_wheres.empty?

      merged_where_nots = {}
      # This will merge all the where not clauses into a single hash.  If the same attribute is
      # specified multiple times, the last one will win.
      (@where_not_values + r.where_not_values).each { |w| merged_where_nots.merge!(w) }

      merged_relation.where_not_values = [merged_where_nots] unless merged_where_nots.empty?

      (Relation::SINGLE_VALUE_METHODS).each do |method|
        value = r.send(:"#{method}_value")
        merged_relation.send(:"#{method}_value=", value) unless value.nil? || value == :default
      end

      merged_relation
    end

    # Removes from the query the condition(s) specified in +skips+.
    #
    # Example:
    #
    #   Post.where(:active => true).order('id').except(:order) # discards the order condition
    #   Post.where(:active => true).order('id').except(:where) # discards the where condition but keeps the order
    def except(*skips)
      result = self.class.new(@klass, table)
      result.default_scoped = default_scoped

      ((Relation::ASSOCIATION_METHODS + Relation::MULTI_VALUE_METHODS) - skips).each do |method|
        result.send(:"#{method}_values=", send(:"#{method}_values"))
      end

      (Relation::SINGLE_VALUE_METHODS - skips).each do |method|
        result.send(:"#{method}_value=", send(:"#{method}_value"))
      end

      # Apply scope extension modules
      result.send(:apply_modules, extensions)

      result
    end

    # Removes any condition from the query other than the one(s) specified in +onlies+.
    #
    # Example:
    #
    #   Post.order('id').only(:where)         # discards the order condition
    #   Post.order('id').only(:where, :order) # uses the specified order
    #
    def only(*onlies)
      result = self.class.new(@klass, table)
      result.default_scoped = default_scoped

      ((Relation::ASSOCIATION_METHODS + Relation::MULTI_VALUE_METHODS) & onlies).each do |method|
        result.send(:"#{method}_values=", send(:"#{method}_values"))
      end

      (Relation::SINGLE_VALUE_METHODS & onlies).each do |method|
        result.send(:"#{method}_value=", send(:"#{method}_value"))
      end

      # Apply scope extension modules
      result.send(:apply_modules, extensions)

      result
    end

    VALID_FIND_OPTIONS = %i(conditions limit select offset order group page per_page fulltext consistency with_solr
                            with_cassandra where where_not)
    # Applies the passed in finder options and returns a new Relation.
    # Takes any of the options below and calls them on the relation as if they
    # were methods (+conditions+ is passed to +where+).
    #
    # @param [Hash] options the options hash
    # @option options [Hash] :conditions
    # @option options [Symbol, String] :consistency
    # @option options [String] :fulltext
    # @option options [Symbol, String] :group
    # @option options [Integer, String] :limit
    # @option options [Integer, String] :offset
    # @option options [String, Hash] :order
    # @option options [Integer, String] :page
    # @option options [Integer, String] :per_page
    # @option options [Array] :select
    # @option options [Hash] :where
    # @option options [Hash] :where_not
    # @option options [Boolean] :with_cassandra
    # @option options [Boolean] :with_solr
    # @return [DatastaxRails::Relation] relation with all options applied
    # @raise [ArgumentError] if an invalid option is passed in
    def apply_finder_options(options)
      relation = self
      return relation unless options

      options.assert_valid_keys(VALID_FIND_OPTIONS)
      finders = options.dup
      finders.delete_if { |_key, value| value.nil? }

      ((VALID_FIND_OPTIONS - [:conditions]) & finders.keys).each do |finder|
        if finder.to_s =~ /(with_solr|with_cassandra)/
          relation = relation.send(finder)
        else
          relation = relation.send(finder, finders[finder])
        end
      end

      relation = relation.where(finders[:conditions]) if finders.key?(:conditions)
      relation
    end
  end
end
