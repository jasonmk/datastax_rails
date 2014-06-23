module DatastaxRails
  module StatementCache
    extend ActiveSupport::Concern
    
    included do
      class_attribute :statement_cache
      self.statement_cache = {}
    end
  end
end