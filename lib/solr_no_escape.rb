require 'sunspot'
module SolrNoEscape
  def escape(str)
    # We are purposely not escaping since we want people to be able to do
    # advanced queries that otherwise wouldn't work.  Spaces are a special
    # case due to later URL escaping.
    str.gsub(/ /,"\\\\ ")
  end
end

module Sunspot
  module Query
    class FunctionQuery
      include SolrNoEscape
    end
  end
end

module Sunspot
  module Query
    module Restriction
      class Base
        include SolrNoEscape
      end
    end
  end
end

