module DatastaxRails
  # A special model that is designed to efficiently store very wide data.
  # This model type assumes that you have a unique ID and want to either
  # search or sort on a second piece of data.  The data is stored as a
  # single, very wide row; however you can safely treat it as if
  # there were multiple rows.
  #
  # You can also apply secondary indexes onto the other columns.
  #
  # CAVEATS: 
  # * Wide Storage Models cannot be indexed into Solr (yet).
  # * Once the cluster is set, it cannot be changed as it becomes the column header in Cassandra
  #
  #   class AuditLog < DatastaxRails::WideStorageModel
  #     self.column_family = 'audit_logs'
  #     self.primary_key = :uuid
  #     self.cluster_by = :created_at
  #     # If you don't want the default ascending sort order
  #     self.create_options = 'CLUSTERING ORDER BY (created_at DESC)' 
  #
  #     string :uuid
  #     string :message
  #     string :user_id, :cql_index => true
  #     timestamps
  #   end
  class WideStorageModel < CassandraOnlyModel
    self.abstract_class = true
    
    class_attribute :cluster_by
    
    def self.write(record, options = {})
      attributes = encode_attributes(record, options)
      level = (options[:consistency] || self.default_consistency).to_s.upcase
      if(valid_consistency?(level))
        options[:consistency] = level
      else
        raise ArgumentError, "'#{level}' is not a valid Cassandra consistency level"
      end
      record.id.tap do |key|
        ActiveSupport::Notifications.instrument("insert.datastax_rails", :column_family => column_family, :key => record.id.to_s, :attributes => attributes) do
            cql.insert.using(level).columns(attributes).execute
        end
      end
    end
  end
end