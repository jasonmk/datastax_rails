module DatastaxRails
  module Cql
    module Consistency
      ONE = 'ONE'
      QUORUM = 'QUORUM'
      LOCAL_QUORUM = 'LOCAL_QUORUM'
      EACH_QUORUM = 'EACH_QUORUM'
      ALL = 'ALL'
      
      VALID_CONSISTENCY_LEVELS = [ONE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL]
    end
  end
end