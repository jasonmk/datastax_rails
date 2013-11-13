module DatastaxRails
  module Schema
    extend ActiveSupport::Autoload

    autoload :Solr
    autoload :Cassandra
    autoload :Migrator
  end
end
