module DatastaxRails
  # A special model that is designed to efficiently store very wide data.
  # This model type makes use of Cassandra compound primary keys.  It
  # cannot be indexed into Solr.
  #
  #   class AuditLog < DatastaxRails::WideStorageModel
  #     self.column_family = 'audit_logs'
  #     key :composite => [:uuid, :created_at]
  #     string :uuid
  #     string :message
  #     timestamps
  #   end
  class WideStorageModel < Base
    
    def self.scoped
      super.with_cassandra
    end
    
    
  end
end