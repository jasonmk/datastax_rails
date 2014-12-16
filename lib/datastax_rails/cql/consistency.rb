module DatastaxRails
  module Cql
    module Consistency
      ANY = :any
      ONE = :one
      LOCAL_ONE = :local_one
      TWO = :two
      THREE = :three
      QUORUM = :quorum
      LOCAL_QUORUM = :local_quorum
      EACH_QUORUM = :each_quorum
      ALL = :all

      VALID_CONSISTENCY_LEVELS = [ANY, ONE, LOCAL_ONE, TWO, THREE, QUORUM, LOCAL_QUORUM, EACH_QUORUM, ALL]
    end
  end
end
