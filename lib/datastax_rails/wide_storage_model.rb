module DatastaxRails
  # A special model that is designed to efficiently store very wide data.
  # This model type assumes that you have a unique ID and want to either
  # search or sort on a second piece of data.  The store the data as a
  # single, very wide row; however you can safely treat them as if
  # there were multiple rows.
  #
  # CAVEATS: 
  # * Wide Storage Models cannot be indexed into Solr.
  # * Once the cluster is set, it cannot be changed as it becomes the column header in Cassandra
  #
  #   class AuditLog < DatastaxRails::WideStorageModel
  #     self.column_family = 'audit_logs'
  #
  #     key :natural, :attributes => [:uuid]
  #     cluster_by :created_at => :desc
  #
  #     string :uuid
  #     string :message
  #     timestamps
  #   end
  class WideStorageModel < Base
    self.abstract_class = true
    
    def self.cluster_by(attr = nil)
      @cluster_by ||= attr.is_a?(Hash) ? attr : {attr => :asc}
    end
    
    def self.scoped
      super.with_cassandra
    end
    
    def self.encode_attributes(attributes)
      encoded = {}
      attributes.each do |column_name, value|
        encoded[column_name.to_s] = attribute_definitions[column_name.to_sym].coder.encode(value)
        if attribute_definitions[column_name.to_sym].coder.options[:cassandra_type] == 'timestamp'
          encoded[column_name.to_s] = encoded[column_name.to_s][0..-2]
        end
      end
      encoded
    end
    
    def self.write(key, attributes, options = {})
      attributes = encode_attributes(attributes)
      level = (options[:consistency] || self.default_consistency).to_s.upcase
      if(valid_consistency?(level))
        options[:consistency] = level
      else
        raise ArgumentError, "'#{level}' is not a valid Cassandra consistency level"
      end
      key.tap do |key|
        ActiveSupport::Notifications.instrument("insert.datastax_rails", :column_family => column_family, :key => key, :attributes => attributes) do
            cql.insert.using(level).columns(attributes).execute
        end
      end
    end
  end
end