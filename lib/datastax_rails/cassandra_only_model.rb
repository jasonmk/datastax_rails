module DatastaxRails
  # A base class designed for models that will only interact with Cassandra.
  # Classes that inherit from this will not generate Solr schemas or have
  # any communication with Solr.  If an application only uses these models
  # then it should be possible to run with pure Cassandra and no Solr at all.
  class CassandraOnlyModel < Base
    self.abstract_class = true
    
    # XXX: This is a complete hack until we properly detect database types
    def self.encode_attributes(attributes)
      encoded = {}
      attributes.each do |column_name, value|
        encoded[column_name.to_s] = attribute_definitions[column_name.to_sym].coder.encode(value)
        if attribute_definitions[column_name.to_sym].coder.options[:cassandra_type] == 'timestamp'
          encoded[column_name.to_s] = encoded[column_name.to_s][0..-2]
        elsif attribute_definitions[column_name.to_sym].coder.options[:cassandra_type] == 'int'
          encoded[column_name.to_s] = encoded[column_name.to_s].to_i
        elsif attribute_definitions[column_name.to_sym].coder.options[:cassandra_type] == 'boolean'
          encoded[column_name.to_s] = encoded[column_name.to_s] == '1'
        end
      end
      encoded
    end
    
    default_scope with_cassandra
  end
end