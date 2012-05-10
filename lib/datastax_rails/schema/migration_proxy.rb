module DatastaxRails
  module Schema

    # MigrationProxy is used to defer loading of the actual migration classes
    # until they are needed
    class MigrationProxy

      attr_accessor :name, :version, :filename

      delegate :migrate, :announce, :write, :to=>:migration

      private

      def migration
        @migration ||= load_migration
      end

      def load_migration
        require(File.expand_path(filename))
        name.constantize
      end

    end
  end
end
