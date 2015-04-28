module DatastaxRails
  # Cache prepared statements locally to avoid needing to re-prepare them
  # over and over on the server.
  module StatementCache
    extend ActiveSupport::Concern

    included do
      class_attribute :statement_cache
      self.statement_cache = {}
    end

    module ClassMethods #:nodoc:
      def establish_connection(spec)
        self.statement_cache = {}
        super
      end
    end
  end
end
