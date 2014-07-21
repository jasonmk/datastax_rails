module DatastaxRails
  module SolrRepair
    def repair_solr
      encoded = {}
      attributes.keys.each do |column_name|
        value = read_attribute(column_name)
        encoded[column_name.to_s] = self.class.column_for_attribute(column_name).type_cast_for_solr(value)
      end
      xml_doc = RSolr::Xml::Generator.new.add(encoded)
      self.class.solr_connection.update(data: xml_doc, params: { replacefields: false })
    end
  end
end
