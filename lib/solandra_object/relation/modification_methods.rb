module DatastaxRails
  module ModificationMethods
    # Destroys the records matching +conditions+ by instantiating each
    # record and calling its +destroy+ method. Each object's callbacks are
    # executed (including <tt>:dependent</tt> association options and
    # +before_destroy+/+after_destroy+ Observer methods). Returns the
    # collection of objects that were destroyed; each will be frozen, to
    # reflect that no changes should be made (since they can't be
    # persisted).
    #
    # Note: Instantiation, callback execution, and deletion of each
    # record can be time consuming when you're removing many records at
    # once. However, it is necessary to perform it this way to ensure
    # that the SOLR index stays in sync with the Cassandra data store.
    #
    # +delete_all+ is aliased to this because you can't delete without running
    # the requisite callbacks. (at least not yet)
    #
    # ==== Parameters
    #
    # * +conditions+ - A string, array, or hash that specifies which records
    # to destroy. If omitted, all records matching the current relation are
    # destroyed. See the Conditions section in the introduction to
    # DatastaxRails::Base for more information.
    #
    # ==== Examples
    #
    # Person.destroy_all(:status => "inactive")
    # Person.where(:age => 0..18).destroy_all
    # Person.where_not(:status => "active").destroy_all
    def destroy_all(conditions = nil)
      if conditions
        where(conditions).destroy_all
      else
        to_a.each {|object| object.destroy }.tap { reset }
      end
    end
    # TODO: Find a way to delete from both without instantiating
    alias :delete_all :destroy_all
    
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
        id.map { |one_id| destroy(one_id) }
      else
        find(id).destroy
      end
    end
    # TODO: Find a way to delete from both without instantiating
    alias :delete :destroy
  end
end