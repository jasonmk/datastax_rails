module DatastaxRails
  # The Cql classes handle all of the generation of CQL. They are constructed in such a way that
  # the statement can be built up over multiple calls before generating the actual CQL.
  #
  # TODO: Add examples
  module Cql
    extend ActiveSupport::Autoload
    class << self
      def for_class(klass)
        @cql ||= {}
        @cql[klass] ||= DatastaxRails::Cql::ColumnFamily.new(klass)
      end
    end

    autoload :AlterColumnFamily
    autoload :Base
    autoload :ColumnFamily
    autoload :Consistency
    autoload :CreateColumnFamily
    autoload :CreateIndex
    autoload :CreateKeyspace
    autoload :Delete
    autoload :DropColumnFamily
    autoload :DropIndex
    autoload :DropKeyspace
    autoload :Insert
    autoload :Select
    autoload :Truncate
    autoload :Update
    autoload :UseKeyspace
  end
end
