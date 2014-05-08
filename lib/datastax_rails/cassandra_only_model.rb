module DatastaxRails
  # A base class designed for models that will only interact with Cassandra.
  # Classes that inherit from this will not generate Solr schemas or have
  # any communication with Solr.  If an application only uses these models
  # then it should be possible to run with pure Cassandra and no Solr at all.
  class CassandraOnlyModel < Base
    self.abstract_class = true
    
    def self.attribute(name, options)
      opts = options.update(:solr_index => false,    :solr_store => false,
                            :multi_valued => false,  :sortable => false,
                            :tokenized => false,     :fulltext => false)
      super(name, opts)
    end
    
    default_scope with_cassandra
  end
end