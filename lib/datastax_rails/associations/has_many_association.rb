module DatastaxRails
  module Associations
    class HasManyAssociation < CollectionAssociation #:nodoc:
      private
      
        # Returns the number of records in this collection.
        #
        # This does not depend on whether the collection has already been loaded
        # or not. The +size+ method is the one that takes the loaded flag into
        # account and delegates to +count_records+ if needed.
        #
        # If the collection is empty the target is set to an empty array and
        # the loaded flag is set to true as well.
        def count_records
          count = scoped.count
          
          # If there's nothing in the database and @target has no new records
          # we are certain the current target is an empty array. This is a
          # documented side-effect of the method that may avoid an extra SELECT.
          @target ||= [] and loaded! if count == 0
          
          count
        end
        
        # Deletes the records according to the <tt>:dependent</tt> option.
        def delete_records(records, method)
          if method == :destroy
            records.each { |r| r.destroy }
          else
            keys = records.map { |r| r[reflection.association_primary_key] }
            scope = scoped.where(reflection.association_primary_key => keys)

            if method == :delete_all
              update_counter(-scope.delete_all)
            else
              # This is for :nullify which isn't actually supported yet,
              # but this should work once it is
              update_counter(-scope.update_all(reflection.foreign_key => nil))
            end
          end
        end
    end
  end
end