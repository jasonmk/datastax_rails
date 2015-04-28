module DatastaxRails
  # Relation methods for destroying an entire collection of records.
  module ModificationMethods
    # Destroys the records matching this relation by instantiating each
    # record and calling its +destroy+ method. Each object's callbacks are
    # executed (including <tt>:dependent</tt> association options and
    # +before_destroy+/+after_destroy+ Observer methods). Returns the
    # collection of objects that were destroyed; each will be frozen, to
    # reflect that no changes should be made (since they can't be
    # persisted).
    #
    # Note: Instantiation, callback execution, and deletion of each
    # record can be time consuming when you're removing many records at
    # once. However, it is necessary to perform it this way since we have
    # to get the results from SOLR (most likely) in order to know what to delete.
    #
    # Person.destroy_all
    # Person.where(:age => 0..18).destroy_all
    # Person.where_not(:status => "active").destroy_all
    def destroy_all
      to_a.each(&:destroy).tap { |_| reset }
    end

    # Like +destroy_all+ but will not run callbacks. It will still have to instantiate the objects.
    def delete_all
      select(klass.primary_key).to_a.each(&:destroy_without_callbacks).tap { |_| reset }
    end

    # Destroy an object (or multiple objects) that has the given id, the object is instantiated first,
    # therefore all callbacks and filters are fired off before the object is deleted. This method is
    # in-efficient since it actually has to instantiate the object just to delte it but allows cleanup
    # methods such as the ones that remove the object from SOLR to be run.
    #
    # This essentially finds the object (or multiple objects) with the given id, creates a new object
    # from the attributes, and then calls destroy on it.
    #
    # Note: this will only find objects that are matched by the current relation even if the ID would
    # otherwise be valid.
    #
    # +delete+ is aliased to this because you can't delete without running
    # the requisite callbacks. (at least not yet)
    #
    # ==== Parameters
    #
    # * +id+ - Can be either an Integer or an Array of Integers.
    #
    # ==== Examples
    #
    # # Destroy a single object
    # Todo.destroy(1)
    #
    # # Destroy multiple objects
    # todos = [1,2,3]
    # Todo.destroy(todos)
    def destroy(id)
      if id.is_a?(Array)
        ret = id.map { |one_id| destroy(one_id) }
      else
        ret = find(id).destroy
      end
      reset
      ret
    end
    alias_method :delete, :destroy
  end
end
