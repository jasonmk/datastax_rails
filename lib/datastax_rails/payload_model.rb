module DatastaxRails
  # A special model that is designed to efficiently store binary files.
  # The major limitation is that the only fields this can store are
  # the SHA1 digest and the payload itself.  If you need to store
  # other metadata, you will need another model that points at this
  # one.
  #
  #   class AttachmentPayload < DatastaxRails::Payload
  #     self.column_family = 'attachment_payloads'
  #
  #     validate do
  #       if self.payload.size > 50.megabytes
  #         errors.add(:payload, "is larger than the limit of 50MB")
  #       end
  #     end
  #   end
  class PayloadModel < Base
    self.abstract_class = true
    
    def self.inherited(child)
      super
      child.key :natural, :attributes => :digest
      child.string :digest
      child.binary :payload
      child.validates :digest, :presence => true
    end
    
    def self.scoped
      super.with_cassandra
    end
    
    def self.find(digest, options = {})
      raise ArgumentError, "'#{options[:consistency]}' is not a valid Cassandra consistency level" unless valid_consistency?(options[:consistency].to_s.upcase) if options[:consistency]
      c = cql.select.conditions(:digest => digest).order('chunk')
      c.using(options[:consistency]) if options[:consistency]
      io = StringIO.new("","w+")
      found = false
      CassandraCQL::Result.new(c.execute).fetch do |row|
        io << Base64.decode64(row.to_hash['payload'])
        found = true
      end
      raise DatastaxRails::RecordNotFound unless found
      io.rewind
      self.instantiate(digest, {:digest => digest, :payload => io.read}, [:digest, :payload])
    end
    
    def self.write(key, attributes, options = {})
      raise ArgumentError, "'#{options[:consistency]}' is not a valid Cassandra consistency level" unless valid_consistency?(options[:consistency].to_s.upcase) if options[:consistency]
      c = self.cql.select("count(*)").conditions(:digest => key)
      count = CassandraCQL::Result.new(c.execute).fetch.to_hash["count"]
      
      i = 0
      io = StringIO.new(attributes['payload'])
      while chunk = io.read(1.megabyte)
        c = cql.insert.columns(:digest => key, :chunk => i, :payload => Base64.encode64(chunk))
        c.using(options[:consistency]) if options[:consistency]
        c.execute
        i += 1
      end
      
      if count and count > i
        i.upto(count) do |j|
          c = cql.delete(key.to_s).key_name('digest').conditions(:chunk => j)
          c.using(options[:consistency]) if options[:consistency]
          c.execute
        end
      end
      
      key
    end
    
    # Instantiates a new object without calling +initialize+.
    #
    # @param [String] key the primary key for the record
    # @param [Hash] attributes a hash containing the columns to set on the record
    # @param [Array] selected_attributes an array containing the attributes that were originally selected from cassandra
    #   to build this object.  Used so that we can avoid lazy-loading attributes that don't exist.
    # @return [DatastaxRails::Base] a model with the given attributes
    def self.instantiate(key, attributes, selected_attributes = [])
      allocate.tap do |object|
        object.instance_variable_set("@loaded_attributes", {}.with_indifferent_access)
        object.instance_variable_set("@key", parse_key(key)) if key
        object.instance_variable_set("@new_record", false)
        object.instance_variable_set("@destroyed", false)
        object.instance_variable_set("@attributes", attributes.with_indifferent_access)
        attributes.keys.each {|k| object.instance_variable_get("@loaded_attributes")[k] = true}
      end
    end
  end
end