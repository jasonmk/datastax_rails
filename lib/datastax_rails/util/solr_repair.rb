module DatastaxRails
  module SolrRepair
    def repair_solr
      my_attrs = self.attributes.symbolize_keys.reject do |k,v|
        v.nil? ||
        !(self.class.attribute_definitions[k].coder.options[:stored] ||
          self.class.attribute_definitions[k].coder.options[:indexed])
      end
      encoded = self.class.encode_attributes(my_attrs).merge(:id => self.id)
      xml_doc = RSolr::Xml::Generator.new.add(encoded)
      self.class.solr_connection.update(:data => xml_doc, :params => {:replacefields => false})
    end
  end
end
