module DatastaxRails
  module Migrations
    extend ActiveSupport::Concern
    extend ActiveSupport::Autoload

    included do
      class_attribute :migrations
      self.migrations = []

      class_attribute :current_schema_version
      self.current_schema_version = 0
    end

    autoload :Migration
    
    class MigrationNotFoundError < StandardError
      def initialize(record_version, migrations)
        super("Cannot migrate a record from #{record_version.inspect}.  Migrations exist for #{migrations.map(&:version)}")
      end
    end
    
    def schema_version
      Integer(@schema_version || self.class.current_schema_version)
    end
    
    module ClassMethods
      def migrate(version, &blk)
        migrations << Migration.new(version, blk)
        
        if version > self.current_schema_version 
          self.current_schema_version = version
        end
      end
    end
  end
end
