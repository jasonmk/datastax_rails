module DatastaxRails
  module Schema #:nodoc:
    extend ActiveSupport::Autoload

    autoload :Solr
    autoload :Cassandra
    autoload :Migrator
  end
end
