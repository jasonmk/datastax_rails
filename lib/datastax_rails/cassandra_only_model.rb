module DatastaxRails
  # A module designed for models that will only interact with Cassandra.
  # Classes that include this will not generate Solr schemas or have
  # any communication with Solr.  If an application only uses these models
  # then it should be possible to run with pure Cassandra and no Solr at all.
  #
  # If you want to search by anything other than primary_key, you will need
  # to add CQL indexes as they are not created by default.
  #
  #   class Model < DatastaxRails::Base
  #     include DatastaxRails::CassandraOnlyModel
  #
  #     uuid :id
  #     string :name, :cql_index => true
  #   end
  module CassandraOnlyModel
    extend ActiveSupport::Concern

    included do
      default_scope -> { with_cassandra }
      self.storage_method = :cql
    end

    module ClassMethods #:nodoc:
      def attribute(name, options)
        opts = options.update(solr_index: false,    solr_store: false,
                              multi_valued: false,  sortable: false,
                              tokenized: false,     fulltext: false)
        super(name, opts)
      end
    end
  end
end
