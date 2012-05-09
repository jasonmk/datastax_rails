module DatastaxRails
  class DatastaxRailsError < StandardError
  end
  
  class AssociationTypeMismatch < DatastaxRailsError
  end
  
  class RecordNotSaved < DatastaxRailsError
  end
  
  class DeleteRestrictionError < DatastaxRailsError
  end
  
  class RecordNotFound < DatastaxRailsError
  end
end