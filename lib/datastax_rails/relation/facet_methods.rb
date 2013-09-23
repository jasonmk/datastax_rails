module DatastaxRails
  module FacetMethods
    # Instructs SOLR to get facet counts on the passed in field.
    #
    #   Model.facet(:category)
    #
    # This may be specified multiple times to get facet counts on multiple fields.
    #
    # @param field [String, Symbol] the field to get facet counts for
    # @return [DatastaxRails::Relation] a new Relation object
    def facet(field)
      clone.tap do |r|
        r.facet_field_values << field
      end
    end
  end
end