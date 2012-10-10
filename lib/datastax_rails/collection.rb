module DatastaxRails
  class Collection < Array
    # @!attribute [r] total_entries
    #   @return [Fixnum] the total number of entries that match the search
    # @!attribute [r] last_column_name
    #   @return [Fixnum] the last column that was returned in the search in case you limited the number of columns (not supported)
    attr_accessor :last_column_name, :total_entries
    
    def inspect
      "<DatastaxRails::Collection##{object_id} contents: #{super} last_column_name: #{last_column_name.inspect}>"
    end
  end
end