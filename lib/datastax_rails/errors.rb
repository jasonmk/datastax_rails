module DatastaxRails
  class DatastaxRailsError < StandardError #:nodoc:
  end

  class AssociationTypeMismatch < DatastaxRailsError #:nodoc:
  end

  class RecordNotSaved < DatastaxRailsError #:nodoc:
  end

  class DeleteRestrictionError < DatastaxRailsError #:nodoc:
  end

  class RecordNotFound < DatastaxRailsError #:nodoc:
  end

  class UnknownAttributeError < DatastaxRailsError
  end
end
