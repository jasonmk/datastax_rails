# Placeholder class to imitate a model for the schema migrations column family.
# Used as part of the CQL generation.
class SchemaMigration
  def self.default_consistency
    DatastaxRails::Base.default_consistency
  end

  # Returns the name of the column family
  def self.column_family
    'schema_migrations'
  end

  def self.primary_key
    'cf'
  end
end
