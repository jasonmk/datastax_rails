module DatastaxRails
  # A special model that is designed to efficiently store very wide data.
  # This model type assumes that you have a unique ID and want to either
  # search or sort on a second piece of data.  The store the data as a
  # single, very wide row; however you can safely treat them as if
  # there were multiple rows.
  #
  # CAVEATS: 
  # * Wide Storage Models cannot be indexed into Solr.
  # * Once the order is set, it cannot be changed as it becomes the column header in Cassandra
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
    
  end
end