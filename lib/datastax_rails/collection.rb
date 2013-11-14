module DatastaxRails
  class Collection < Array
    # @!attribute [r] total_entries
    #   @return [Fixnum] the total number of entries that match the search
    # @!attribute [r] last_column_name
    #   @return [Fixnum] the last column that was returned in the search in case you limited the number of columns (not supported)
    # @!attribute [r] per_page
    #   @return [Fixnum] the per page value of the search that produced these results (used by will_paginate)
    # @!attribute [r] current_page
    #   @return [Fixnum] the current page of the search that produced these results (used by will_paginate)
    # @!attribute [r] facets
    #   @return [Hash] the facet(s) result (field and/or range) e.g. results.facets => {"author"=>["vonnegut", 2, "asimov", 4]} 
    attr_accessor :last_column_name, :total_entries, :per_page, :current_page, :facets, :highlights
    
    def inspect
      "<DatastaxRails::Collection##{object_id} contents: #{super} last_column_name: #{last_column_name.inspect}>"
    end
    
    def total_pages
      return 1 unless per_page
      (total_entries / per_page.to_f).ceil
    end
  end
end