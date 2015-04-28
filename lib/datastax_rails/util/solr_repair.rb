module DatastaxRails
  # Utility method to repair a corrected solr record
  module SolrRepair
    # Pulls all of the attribute values from Cassandra and posts them to Solr as an update
    # This causes all of the solr indexes to get updated with the current values. Can be a
    # quick easy fix for when a record has stale data. Bear in mind that this will update
    # the write timestamp in cassandra and cause the data to be replicated around the cluster.
    #
    # Probably only a good idea for a small subset of records.
    def repair_solr
      encoded = {}
      attribute_definitions.keys.each do |column_name|
        value = read_attribute(column_name)
        encoded[column_name.to_s] = self.class.column_for_attribute(column_name).type_cast_for_solr(value)
      end
      xml_doc = RSolr::Xml::Generator.new.add(encoded)
      self.class.solr_connection.update(data: xml_doc)
    end
  end
end
