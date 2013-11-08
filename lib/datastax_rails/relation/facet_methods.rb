module DatastaxRails
  module FacetMethods
    # Instructs SOLR to get facet counts on the passed in field.  Results are available in the facets accessor.
    # Facets include Field and Range (Date is not supported as it is depricated in Solr).
    #
    # results = Article.field_facet(:author)
    # results.facets => {"author"=>["vonnegut", 2. "asimov", 3]} 
    #
    # Model.field_facet(:author)
    # Model.field_facet(:author, :sort => 'count', :limit => 10, :mincount => 1)
    # Model.range_facet(:price, 500, 1000, 10)
    # Model.range_facet(:price, 500, 1000, 10, :include => 'all')
    # Model.range_facet(:publication_date, "1968-01-01T00:00:00Z", "2000-01-01T00:00:00Z", "+1YEAR")
    #
    # Range Gap syntax for dates: +1YEAR, +5YEAR, +5YEARS, +1MONTH, +1DAY
    #
    # Useful constants:
    #
    # DatastaxRails::FacetMethods::BY_YEAR  (+1YEAR)
    # DatastaxRails::FacetMethods::BY_MONTH (+1MONTH)
    # DatastaxRails::FacetMethods::BY_DAY   (+1DAY)
    #
    # Model.range_facet(:publication_date, "1968-01-01T00:00:00Z", "2000-01-01T00:00:00Z", DatastaxRails::FacetMethods::BY_YEAR)
    #
    # These method can be called multiple times to facet on different fields
    #
    # @param field [String, Symbol] the field to get facet counts for
    # @param options [Hash] the facet options
    # @return [DatastaxRails::Relation] a new Relation object
    #
    # Field Facet Option Values:
    # prefix: [String] Facet prefix
    # limit: [Number] Facet result limit (Solr defaults to 100)
    # sort: [String: count | index] Sort Order (Solr defaults to index, unless a limit is specified, then defaults to index)
    # offset: [Number] Facet result offset (Solr defaults to 0)
    # mincount [Number] Facet minimum number of occurances before including in the result set (Solr defaults to 0)
    # Note: You can pass any valid facet option value, and it will be passed to Solr
    # 
    # Range Facet Option Values:
    # start [String] Range start
    # end: [String] Range end value
    # gap: [String] Range gap (see examples above)
    # include [String: lower | upper | edge | outer | all] (Solr defaults to lower) include / exclude upper and lower range values
    # other [String: before | after | between | none | all] include / exclude counts
    # Note: You can pass any valid facet option value, and it will be passed to Solr
    #    
    # Date Facet? - use Range Facet! They were depricated in Solr 3
    
    BY_YEAR = "+1YEAR"
    BY_MONTH = "+1MONTH"
    BY_DAY = "+1DAY"
    
    def field_facet(field, options = {})
      return self if field.blank?
      clone.tap do |r|
        r.field_facet_values << {:field => field.to_s, :options => options}
      end
    end
    
    def range_facet(field, start_range, end_range, gap, options = {})
      return self if field.blank?
      clone.tap do |r|
        r.range_facet_values << {:field => field.to_s, :options => options.merge(:start => start_range.to_s, :end => end_range.to_s, :gap => gap.to_s)}
      end
    end
  
  end
end