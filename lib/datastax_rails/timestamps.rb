module DatastaxRails
  module Timestamps
    extend ActiveSupport::Concern

    included do
      # attribute :created_at, :type => :time#_with_zone
      # attribute :updated_at, :type => :time#_with_zone

      before_create do #|r|
        if self.respond_to?(:created_at=)
          self.created_at ||= Time.current
        end
        if self.respond_to?(:updated_at=)
          self.updated_at ||= Time.current
        end
      end

      before_update :if => :changed? do #|r|
        if self.respond_to?(:updated_at=)
          self.updated_at = Time.current
        end
      end
    end
  end
end
