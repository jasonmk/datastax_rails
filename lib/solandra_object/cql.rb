module DatastaxRails
  module Cql
    extend ActiveSupport::Autoload
    class << self
      def for_class(klass)
        @cql ||= {}
        @cql[klass] ||= DatastaxRails::Cql::ColumnFamily.new(klass)
      end
    end
    
    autoload :Base
    autoload :ColumnFamily
    autoload :Consistency
    autoload :CreateColumnFamily
    autoload :CreateKeyspace
    autoload :Delete
    autoload :DropColumnFamily
    autoload :DropKeyspace
    autoload :Insert
    autoload :Select
    autoload :Truncate
    autoload :Update
    autoload :UseKeyspace
  end
end