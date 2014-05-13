module DatastaxRails
  # An extension to Wide Storage Models that let you index any arbitrary
  # field that you want (given certain naming conventions).
  #
  # Try to keep the group_by as sort as possible since it will get stored
  # with every attribute.
  #
  #   class CoreMetadata < DatastaxRails::DynamicModel
  #     self.group_by = 'core'
  #   end
  #
  #   class TeamMetadata < DatastaxRails::DynamicModel
  #     self.group_by = 'team'
  #   end
  class DynamicModel < WideStorageModel
    self.abstract_class = true
    
    class_attribute :group_by_attribute
    
    def self.group_by=(group)
      self.group_by_attribute = group
      self.attribute_definitions['group'].default = group
      default_scope -> {where('group' => group)}
    end
    
    
    def self.inherited(child)
      super
      child.column_family = 'dynamic_model'
      child.primary_key = 'id'
      child.cluster_by = 'group'
      child.uuid :id
      child.string :group
      child.map :s_,  :holds => :string
      child.map :t_,  :holds => :text
      child.map :b_,  :holds => :boolean
      child.map :d_,  :holds => :date
      child.map :ts_, :holds => :timestamp
      child.map :i_,  :holds => :integer
      child.map :f_,  :holds => :float
      child.map :u_,  :holds => :uuid
      
      child.map_columns.each do |col|
        child.instance_eval do
          alias_attribute col.options[:holds].to_s.pluralize, col.name
        end
      end
    end
  end
end