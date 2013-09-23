module DatastaxRails
  module FinderMethods
    # Find operates with four different retrieval approaches:
    #
    # * Find by id - This can either be a specific id (1), a list of ids (1, 5, 6), or an array of ids ([5, 6, 10]).
    #   If no record can be found for all of the listed ids, then RecordNotFound will be raised.
    # * Find first - This will return the first record matched by the options used. These options can either be specific
    #   conditions or merely an order. If no record can be matched, +nil+ is returned. Use
    #   <tt>Model.find(:first, *args)</tt> or its shortcut <tt>Model.first(*args)</tt>.
    # * Find last - This will return the last record matched by the options used. These options can either be specific
    #   conditions or merely an order. If no record can be matched, +nil+ is returned. Use
    #   <tt>Model.find(:last, *args)</tt> or its shortcut <tt>Model.last(*args)</tt>.
    # * Find all - This will return all the records matched by the options used.
    #   If no records are found, an empty array is returned. Use
    #   <tt>Model.find(:all, *args)</tt> or its shortcut <tt>Model.all(*args)</tt>.
    #
    # All approaches accept an options hash as their last parameter.
    #
    # ==== Options
    #
    # * <tt>:conditions</tt> - See conditions in the intro.
    # * <tt>:order</tt> - An SQL fragment like "created_at DESC, name".
    # * <tt>:group</tt> - An attribute name by which the result should be grouped. Uses the <tt>GROUP BY</tt> SQL-clause.
    # * <tt>:limit</tt> - An integer determining the limit on the number of rows that should be returned.
    # * <tt>:offset</tt> - An integer determining the offset from where the rows should be fetched. So at 5,
    #   it would skip rows 0 through 4.
    #
    # ==== Examples
    #
    # # find by id
    #   Person.find(1) # returns the object for ID = 1
    #   Person.find(1, 2, 6) # returns an array for objects with IDs in (1, 2, 6)
    #   Person.find([7, 17]) # returns an array for objects with IDs in (7, 17)
    #   Person.find([1]) # returns an array for the object with ID = 1
    #   Person.where(:administrator => 1).order(:created_on => :desc).find(1)
    #
    # Note that the returned order is undefined unless you give a specific +:order+ clause.
    # Further note that order is handled in memory and so does suffer a performance penalty.
    #
    # ==== Examples
    #
    #   # find first
    #   Person.first # returns the first object fetched by SELECT * FROM people
    #   Person.where(:user_name => user_name).first
    #   Person.order(:created_on => :desc).offset(5).first
    #
    #   # find last
    #   Person.last # returns the last object in the column family
    #   Person.where(:user_name => user_name).last
    #   Person.order(:created_at => :desc).offset(5).last
    #
    #   # find all
    #   Person.all # returns an array of objects for all the rows in the column family
    #   Person.where(["category IN (?)", categories]).limit(50).all
    #   Person.where(:friends => ["Bob", "Steve", "Fred"]).all
    #   Person.offset(10).limit(10).all
    #   Person.group("category").all
    def find(*args)
      return to_a.find { |*block_args| yield(*block_args) } if block_given?

      options = args.extract_options!
      if options.present?
        apply_finder_options(options).find(*args)
      else
        case args.first
        when :first, :last, :all
          send(args.first)
        else
          self.use_solr_value = false
          find_with_ids(*args)
        end
      end
    end
    
    # A convenience wrapper for <tt>find(:first, *args)</tt>. You can pass in all the
    # same arguments to this method as you can to <tt>find(:first)</tt>.
    def first(*args)
      if args.any?
        if args.first.kind_of?(Integer) || (loaded? && !args.first.kind_of?(Hash))
          limit(*args).to_a
        else
          apply_finder_options(args.first).first
        end
      else
        find_first
      end
    end
    
    # Same as +first+ but raises <tt>DatastaxRails::RecordNotFound</tt> if no record
    # is found. Note that <tt>first!</tt> accepts no arguments.
    def first!
      first or raise RecordNotFound
    end
    
    # A convenience wrapper for <tt>find(:last, *args)</tt>. You can pass in all the
    # same arguments to this method as you can to <tt>find(:last)</tt>.
    def last(*args)
      if args.any?
        if args.first.kind_of?(Integer) || (loaded? && !args.first.kind_of?(Hash))
          if order_values.empty? && reorder_value.nil?
            order(:id => :desc).limit(*args).reverse
          else
            to_a.last(*args)
          end
        else
          apply_finder_options(args.first).last
        end
      else
        find_last
      end
    end

    # Same as +last+ but raises <tt>DatastaxRails::RecordNotFound</tt> if no record
    # is found. Note that <tt>last!</tt> accepts no arguments.
    def last!
      last or raise RecordNotFound
    end
    
    private
      def find_with_ids(*ids)
        return to_a.find { |*block_args| yield(*block_args) } if block_given?
  
        expects_array = ids.first.kind_of?(Array)
        return ids.first if expects_array && ids.first.empty?
  
        ids = ids.flatten.compact.uniq
  
        case ids.size
        when 0
          raise RecordNotFound, "Couldn't find #{@klass.name} without an ID"
        when 1
          result = find_one(ids.first)
          expects_array ? [ result ] : result
        else
          find_some(ids)
        end
      end
      
      def find_one(id)
        with_cassandra.where(:key => id).first || raise(RecordNotFound, "Couldn't find #{@klass.name} with ID=#{id}")
      end
  
      def find_some(ids)
        result = with_cassandra.where(:key => ids).all
  
        expected_size =
          if @limit_value && ids.size > @limit_value
            @limit_value
          else
            ids.size
          end
  
        # 11 ids with limit 3, offset 9 should give 2 results.
        if @offset_value && (ids.size - @offset_value < expected_size)
          expected_size = ids.size - @offset_value
        end
  
        if result.size == expected_size
          result
        else
          error = "Couldn't find all #{@klass.name.pluralize} with IDs "
          error << "(#{ids.join(", ")}) (found #{result.size} results, but was looking for #{expected_size})"
          raise RecordNotFound, error
        end
      end
    
      def find_first
        if loaded?
          @results.first
        else
          @first ||= limit(1).to_a[0]
        end
      end
      
      def find_last
        if loaded?
          @results.last
        else
          @last ||= reverse_order.limit(1).to_a[0]
        end
      end
  end
end