module DatastaxRails
  module StatementCache
    extend ActiveSupport::Concern

    included do
      class_attribute :statement_cache
      self.statement_cache = {}
    end

    module ClassMethods
      def establish_connection(spec)
        self.statement_cache = {}
        super
      end
    end
  end
end
