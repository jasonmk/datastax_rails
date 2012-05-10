require 'active_support/all'
require 'cassandra-cql/1.0'

module DatastaxRails
  extend ActiveSupport::Autoload
  
  autoload :Associations
  autoload :AttributeMethods
  autoload :Base
  autoload :Batches
  autoload :Callbacks
  autoload :CassandraFinderMethods
  autoload :Collection
  autoload :Connection
  autoload :Consistency
  autoload :Cql
  autoload :Cursor
  autoload :Identity
  autoload :Migrations
  #autoload :Mocking
  autoload :Persistence
  autoload :Reflection
  autoload :Relation
  
  autoload_under 'relation' do
    autoload :FinderMethods
    autoload :ModificationMethods
    autoload :SearchMethods
    autoload :SpawnMethods
  end
  
  autoload :Schema
  autoload :Scoping
  autoload :Serialization
  autoload :SunspotAdapters
  autoload :Timestamps
  autoload :Type
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

# Fixup the thrift library
require "thrift"
module Thrift
  class BinaryProtocol
    def write_string(str)
      write_i32(str.bytesize)
      trans.write(str)
    end
  end
end

require 'datastax_rails/railtie' if defined?(Rails)
require 'datastax_rails/errors'
# require 'solr_no_escape'

ActiveSupport.run_load_hooks(:datastax_rails, DatastaxRails::Base)