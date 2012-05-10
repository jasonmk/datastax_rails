module DatastaxRails
  module Schema
    extend ActiveSupport::Autoload

    class IrreversibleMigration < StandardError
    end

    class DuplicateMigrationVersionError < StandardError#:nodoc:
      def initialize(version)
        super("Multiple migrations have the version number #{version}")
      end
    end

    class DuplicateMigrationNameError < StandardError#:nodoc:
      def initialize(name)
        super("Multiple migrations have the name #{name}")
      end
    end

    class UnknownMigrationVersionError < StandardError #:nodoc:
      def initialize(version)
        super("No migration with version number #{version}")
      end
    end

    class IllegalMigrationNameError < StandardError#:nodoc:
      def initialize(name)
        super("Illegal name for migration file: #{name}\n\t(only lower case letters, numbers, and '_' allowed)")
      end
    end

    autoload :Migrator
    autoload :Migration
    autoload :MigrationProxy

  end
end
