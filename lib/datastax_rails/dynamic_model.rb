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
  #     self.grouping = 'item'
  #     timestamps
  #   end
  #
  #   class CoreMetadata < DatastaxRails::DynamicModel
  #     self.grouping = 'core'
  #     timestamps
  #   end
  #
  #   class TeamMetadata < DatastaxRails::DynamicModel
  #     self.grouping = 'team'
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
  #
  # If you would like to still define known attributes ahead of time, you can still do so:
  #
  #   class TeamMetadata < DatastaxRails::DynamicModel
  #     self.grouping = 'team'
  #     string :name
  #     timestamps
  #   end
  #
  #   TeamMetadata.new(name: 'John').name #=> 'John'
  #   TeamMetadata.new(name: 'John').strings #=> {'s_name' => 'John'}
  #
  # Getters and setters are automatically created to map to the attribute stored in the hash.
  # In addition, there is a helper method to map column names to the field name in solr to
  # assist with search.
  class DynamicModel < WideStorageModel
    self.abstract_class = true

    PREFIXES = { string: :s_, text: :t_, boolean: :b_, date: :d_, long: :l_, double: :db_,
                 timestamp: :ts_, integer: :i_, float: :f_, uuid: :u_ }.with_indifferent_access

    class_attribute :group_by_attribute
    class_attribute :virtual_attributes

    class << self
      def grouping=(group)
        self.group_by_attribute = group
        attribute_definitions['group'].default = group
        default_scope -> { where('group' => group) }
      end

      alias_method :_attribute, :attribute

      def attribute(name, options)
        options.symbolize_keys!
        unless [:map, :list, :set].include?(options[:type].to_sym)
          # Only type supported for now
          options.assert_valid_keys(:type)
          unless PREFIXES.key?(options[:type])
            fail(ArgumentError, "Invalid type specified for dynamic attribute: '#{name}: #{options[:type]}'")
          end
          virtual_attributes[name.to_s] = PREFIXES[options[:type]].to_s + name.to_s
        end
        super
      end

      def inherited(child)
        super
        child.virtual_attributes =
          child.virtual_attributes.nil? ? {}.with_indifferent_access : child.virtual_attributes.dup
        child.column_family = 'dynamic_model'
        child.primary_key = 'id'
        child.cluster_by = 'group'
        child._attribute :id, type: :uuid
        child._attribute :group, type: :string
        PREFIXES.each do |k, v|
          child._attribute v, holds: k.to_sym, type: :map
          child.instance_eval { alias_attribute k.to_s.pluralize, v }
        end
      end

      def solr_field_name(attr, type = nil)
        type ||= attribute_definitions[attr].try(:type)
        fail(UnknownAttributeError, 'Collections cannot be mapped') if [:map, :list, :set].include?(type)
        fail(UnknownAttributeError, "Unknown attribute: #{attr}. You must specify a type.") unless type
        PREFIXES[type].to_s + attr.to_s
      end
    end

    def write_attribute(attr_name, val)
      if virtual_attributes.include?(attr_name.to_s)
        type = self.class.attribute_definitions[attr_name].type
        send(PREFIXES[type])[attr_name] = val
      else
        super
      end
    end

    def read_attribute(attr_name)
      if virtual_attributes.include?(attr_name.to_s)
        type = self.class.attribute_definitions[attr_name].type
        send(PREFIXES[type])[attr_name]
      else
        super
      end
    end

    def solr_field_name(attr, type = nil)
      self.class.solr_field_name(attr, type)
    end
  end
end
