require 'active_support/all'
require 'cassandra-cql/1.2'
require 'blankslate'
require 'schema_migration'

# Welcome to DatastaxRails.  DatastaxRails::Base is probably a good place to start.
module DatastaxRails
  extend ActiveSupport::Autoload
  
  autoload :Associations
  autoload :AttributeMethods
  autoload :Base
  autoload :Batches
  autoload :Callbacks
  autoload :Collection
  autoload :Connection
  autoload :Cql
  autoload :GroupedCollection
  autoload :Identity
  autoload :Inheritance
  autoload :PayloadModel
  autoload :Persistence
  autoload :Reflection
  autoload :Relation
  
  autoload_under 'relation' do
    autoload :FinderMethods
    autoload :ModificationMethods
    autoload :SearchMethods
    autoload :SpawnMethods
    autoload :StatsMethods
    autoload :Batches
    autoload :FacetMethods
  end
  
  autoload :RSolrClientWrapper, 'datastax_rails/rsolr_client_wrapper'
  autoload :Schema
  autoload :Scoping
  autoload :Serialization
  autoload :Timestamps
  autoload :Type
  autoload_under 'util' do
    autoload :SolrRepair
  end
  autoload :Validations
  autoload :Version
  autoload :WideStorageModel
  
  module AttributeMethods
    extend ActiveSupport::Autoload

    eager_autoload do
      autoload :Definition
      autoload :Dirty
      autoload :Typecasting
    end
  end

  module Tasks
    extend ActiveSupport::Autoload
    autoload :Keyspace
    autoload :ColumnFamily
  end

  module Types
    extend ActiveSupport::Autoload
    
    autoload :BaseType
    autoload :BinaryType
    autoload :ArrayType
    autoload :BooleanType
    autoload :DateType
    autoload :FloatType
    autoload :IntegerType
    autoload :JsonType
    autoload :StringType
    autoload :TextType
    autoload :TimeType
    autoload :TimeWithZoneType
  end
end

require 'datastax_rails/railtie' if defined?(Rails)
require 'datastax_rails/errors'

ActiveSupport.run_load_hooks(:datastax_rails, DatastaxRails::Base)
