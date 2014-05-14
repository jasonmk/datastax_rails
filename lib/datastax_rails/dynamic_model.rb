module DatastaxRails
  # An extension to Wide Storage Models that let you index any arbitrary
  # field that you want (given certain naming conventions).
  #
  # Try to keep the group_by as sort as possible since it will get stored
  # with every attribute.  Static attributes are only supported if they
  # are included on every dynamic model that uses the same column family.
  #
  # Dynamic models have the following attributes:
  # * strings
  # * texts
  # * booleans
  # * dates
  # * timestamps
  # * integers
  # * floats
  # * uuids
  #
  # Each of these is a map that let's you store key/value pairs where the
  # key is always a String and the value is a type that matches what would
  # be stored in a static attribute of the same time.  Everything will get
  # typecasted, so you can safely store strings in it in all the same cases
  # that you store strings in normal attributes.
  #
  # The advantage here is that you don't have to pre-define your schema
  # ahead of time. The keys of any attributes added to this collection become
  # fields in your Solr document.
  #
  # NOTE: due to the way fields dynamically map between Solr and Cassandra,
  # the field name in Solr will have a prefix prepended to it. With the
  # exception of timestamps, it is simply the first letter of the type
  # followed by an underscore (_). So s_ for strings. Timestamp has a
  # ts_ prefix to differentiate it from texts.
  #   
  #   class Item < DatastaxRails::DynamicModel
  #     self.group_by = 'item'
  #     timestamps
  #   end
  #
  #   class CoreMetadata < DatastaxRails::DynamicModel
  #     self.group_by = 'core'
  #     timestamps
  #   end
  #
  #   class TeamMetadata < DatastaxRails::DynamicModel
  #     self.group_by = 'team'
  #     timestamps
  #   end
  #
  #   item = Item.create(strings: {title: "Title"})
  #   CoreMetadata.create(id: item.id, strings: {author: 'John'}, dates: {published_on: Date.today})
  #   TeamMetadata.create(id: item.id, booleans: {reviewed: true})
  #
  #   CoreMetadata.where(s_author: 'John') #=> Finds the CoreMetadata record
  #   Item.fulltext("Title") #=> Finds the Item record
  #   Item.fulltext("John") #=> Doesn't find a record, but...
  #   Item.fulltext("{!join from=id to=id}John") #=> Does find the record by doing a Solr join across the entire row
  #
  # NOTE that the mapping of key names is happening automatically when you insert something into
  # the collection so:
  #
  #   Item.first.strings #=> {s_title: "Title"}
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