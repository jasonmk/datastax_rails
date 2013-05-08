require 'active_support/all'
require 'cassandra-cql/1.1'
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
  autoload :Migrations
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

require "thrift"
# Thrift is how we communicate with Cassandra.  We need to do a little fixup
# work to handle UTF-8 properly in Ruby 1.8.6.
module Thrift
  class BinaryProtocol
    def write_string(str)
      if(str.respond_to?(:bytesize))
        size = str.bytesize
      else
        size = str.size
      end
      write_i32(size)
      trans.write(str)
    end
  end
end

require 'datastax_rails/railtie' if defined?(Rails)
require 'datastax_rails/errors'

ActiveSupport.run_load_hooks(:datastax_rails, DatastaxRails::Base)
