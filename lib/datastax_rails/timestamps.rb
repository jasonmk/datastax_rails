module DatastaxRails
  module Timestamps
    extend ActiveSupport::Concern

    included do
      # attribute :created_at, :type => :time#_with_zone
      # attribute :updated_at, :type => :time#_with_zone

      before_create do #|r|
        self.created_at ||= Time.current
        self.updated_at ||= Time.current
      end

      before_update :if => :changed? do #|r|
        self.updated_at = Time.current
      end
    end
  end
end
