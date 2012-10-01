module DatastaxRails
  class GroupedCollection < Hash
    attr_accessor :total_entries, :total_groups, :total_for_all
    
    def inspect
      "<DatastaxRails::GroupedCollection##{object_id} contents: #{super} matching_records #{total_for_all}>"
    end
  end
end