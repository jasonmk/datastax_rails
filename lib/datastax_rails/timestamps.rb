module DatastaxRails
  # = DatastaxRails Timestamps
  #
  # DatastaxRails automatically timestamps create and update operations if the
  # table has fields named <tt>created_at</tt> or <tt>updated_at</tt>.
  #
  # Timestamping can be turned off by setting:
  #
  #   DatastaxRails::Base.record_timestamps = false
  module Timestamps
    extend ActiveSupport::Concern

    included do
      class_attribute :record_timestamps
      self.record_timestamps = true
    end

    def initialize_dup(other) # :nodoc:
      clear_timestamp_attributes
      super
    end

    private

    def _create_record(*args)
      if record_timestamps
        current_time = current_time_from_proper_timezone

        all_timestamp_attributes.each do |column|
          if respond_to?(column) && respond_to?("#{column}=") && send(column).nil?
            write_attribute(column.to_s, current_time)
          end
        end
      end

      super
    end

    def _update_record(*args)
      if should_record_timestamps?
        current_time = current_time_from_proper_timezone

        timestamp_attributes_for_update_in_model.each do |column|
          column = column.to_s
          next if attribute_changed?(column)
          write_attribute(column, current_time)
        end
      end
      super
    end

    def should_record_timestamps?
      record_timestamps && (changed? || (attributes.keys & self.class.serialized_attributes.keys).present?)
    end

    def timestamp_attributes_for_create_in_model
      timestamp_attributes_for_create.select { |c| self.class.column_names.include?(c.to_s) }
    end

    def timestamp_attributes_for_update_in_model
      timestamp_attributes_for_update.select { |c| self.class.column_names.include?(c.to_s) }
    end

    def all_timestamp_attributes_in_model
      timestamp_attributes_for_create_in_model + timestamp_attributes_for_update_in_model
    end

    def timestamp_attributes_for_update
      [:updated_at]
    end

    def timestamp_attributes_for_create
      [:created_at]
    end

    def all_timestamp_attributes
      timestamp_attributes_for_create + timestamp_attributes_for_update
    end

    def max_updated_column_timestamp
      if (timestamps = timestamp_attributes_for_update.map { |attr| self[attr] }.compact).present?
        timestamps.map { |ts| ts.to_time }.max
      end
    end

    def current_time_from_proper_timezone
      self.class.default_timezone == :utc ? Time.now.utc : Time.now
    end

    # Clear attributes and changed_attributes
    def clear_timestamp_attributes
      all_timestamp_attributes_in_model.each do |attribute_name|
        self[attribute_name] = nil
        changed_attributes.delete(attribute_name)
      end
    end
  end
end
