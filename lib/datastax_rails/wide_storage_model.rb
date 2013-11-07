module DatastaxRails
  # A special model that is designed to efficiently store very wide data.
  # This model type assumes that you have a unique ID and want to either
  # search or sort on a second piece of data.  The store the data as a
  # single, very wide row; however you can safely treat them as if
  # there were multiple rows.
  #
  # Wide Storage Models cannot be indexed into Solr.
  #
  #   class AuditLog < DatastaxRails::WideStorageModel
  #     self.column_family = 'audit_logs'
  #
  #     key :natural, :attributes => [:uuid]
  #     order  :created_at
  #
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