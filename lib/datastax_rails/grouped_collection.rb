module DatastaxRails
  # GroupedCollection extends Hash to add some additional metadata.  The hash keys will be the values
  # for the thing that was grouped on.  The hash entries point to instances of DatastaxRails::Collection.
  class GroupedCollection < Hash
    # @!attribute [r] total_entries
    #   @return [Fixnum] the total number of entries *in the largest group*.  This is to allow will_paginate to work properly.
    # @!attribute [r] total_groups
    #   @return [Fixnum] the total number of groups if the groups were paginated (not supported yet)
    # @!attribute [r] total_for_all
    #   @return [Fixnum] the total number of entries across all groups that match this search
    attr_accessor :total_entries, :total_groups, :total_for_all
    
    def inspect
      "<DatastaxRails::GroupedCollection##{object_id} contents: #{super} matching_records #{total_for_all}>"
    end
  end
end