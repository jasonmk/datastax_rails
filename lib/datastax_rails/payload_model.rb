module DatastaxRails
  # A special model that is designed to efficiently store binary files.
  # The major limitation is that the only fields this can store are
  # the SHA1 digest and the payload itself.  If you need to store
  # other metadata, you will need another model that points at this
  # one.
  #
  #   class AttachmentPayload < DatastaxRails::PayloadModel
  #     self.column_family = 'attachment_payloads'
  #
  #     validate do
  #       if self.payload.size > 50.megabytes
  #         errors.add(:payload, "is larger than the limit of 50MB")
  #       end
  #     end
  #   end
  class PayloadModel < WideStorageModel
    self.abstract_class = true
    
    def self.inherited(child)
      super
      child.primary_key = 'digest'
      child.cluster_by = 'chunk'
      child.create_options = 'COMPACT STORAGE'
      child.string :digest
      child.binary :payload
      child.integer :chunk
      child.validates :digest, :presence => true
    end
    
    def self.find(digest, options = {})
      raise ArgumentError, "'#{options[:consistency]}' is not a valid Cassandra consistency level" unless valid_consistency?(options[:consistency].to_s.upcase) if options[:consistency]
      c = cql.select.conditions(:digest => digest).order('chunk')
      c.using(options[:consistency]) if options[:consistency]
      io = StringIO.new("","w+")
      found = false
      chunk = 0
      c.execute.each do |row|
        io << Base64.decode64(row['payload'])
        chunk = row['chunk']
        found = true
      end
      raise DatastaxRails::RecordNotFound unless found
      io.rewind
      self.instantiate(digest, {:digest => digest, :payload => io.read, :chunk => chunk}, [:digest, :payload])
    end
    
    def self.write(record, options = {})
      raise ArgumentError, "'#{options[:consistency]}' is not a valid Cassandra consistency level" unless valid_consistency?(options[:consistency].to_s.upcase) if options[:consistency]
      c = self.cql.select("count(*)").conditions(:digest => record.id)
      count = c.execute.first["count"]
      
      i = 0
      io = StringIO.new(record.attributes['payload'])
      while chunk = io.read(1.megabyte)
        c = cql.insert.columns(:digest => record.id, :chunk => i, :payload => Base64.encode64(chunk))
        c.using(options[:consistency]) if options[:consistency]
        c.execute
        i += 1
      end
      
      if count and count > i
        i.upto(count) do |j|
          c = cql.delete(record.id).key_name('digest').conditions(:chunk => j)
          c.using(options[:consistency]) if options[:consistency]
          c.execute
        end
      end
      
      record.id
    end
  end
end