module DatastaxRails
  # A special model that is designed to efficiently store very wide data.
  # This model type assumes that you have a unique ID that you want to
  # store a lot of data about.  The data is stored as a single, very 
  # wide row; however you can safely treat it as if there were multiple rows.
  # A common example of this is a logs table
  #
  # You can also apply secondary indexes onto the other columns.
  #
  # CAVEATS: 
  # * Once the cluster attribute is set, it cannot be changed as it becomes the column header in Cassandra
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
  class WideStorageModel < DatastaxRails::Base
    self.abstract_class = true
    
    # Returns a primary key hash for updates that includes the cluster key
    def id_for_update
      {self.class.primary_key.to_s => self.id, self.class.cluster_by.to_s => self.read_attribute(self.class.cluster_by.to_s)}
    end
  end
end