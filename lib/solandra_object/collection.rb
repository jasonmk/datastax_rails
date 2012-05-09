module DatastaxRails
  class Collection < Array
    attr_accessor :last_column_name, :total_entries
    
    def inspect
      "<DatastaxRails::Collection##{object_id} contents: #{super} last_column_name: #{last_column_name.inspect}>"
    end
  end
end