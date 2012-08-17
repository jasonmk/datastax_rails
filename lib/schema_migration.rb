# Placeholder class to imitate a model for the schema migrations column family.
# Used as part of the CQL generation.
class SchemaMigration
  class_attribute :default_consistency
  self.default_consistency = :quorum
  # Returns the name of the column family
  def self.column_family
    'schema_migrations'
  end
end