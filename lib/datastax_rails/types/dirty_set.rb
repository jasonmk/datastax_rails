module DatastaxRails
  module Types
    class DirtySet < DirtyList
      
      private
        def organize_collection
          Array.instance_method(:compact!).bind(self).call
          Array.instance_method(:uniq!).bind(self).call
        end
    end
  end
end