module DatastaxRails
  # = DatastaxRails Has And Belongs To Many Association
  module Associations
    class HasAndBelongsToManyAssociation < CollectionAssociation #:nodoc:
      attr_reader :join_name

      def initialize(owner, reflection)
        self.join_name = [owner.class.name.underscore, reflection.class_name.underscore].sort.join('_')
        super
      end

      def insert_record(record, validate = true, raise = false)
        if record.new_record?
          if raise
            record.save!(validate: validate)
          else
            return unless record.save(validate: validate)
          end
        end

        left, right = [[record.class.name, record.id], [owner.class.name, owner.id]].sort { |a, b| b.first <=> a.first }

        DatastaxRails::Base.connection.insert(join_column_family,
                                              SimpleUUID::UUID.new.to_guid,
                                              left:  "#{join_name}:#{left.last}",
                                              right: "#{join_name}:#{right.last}")
      end

      private

      def join_column_family
        'many_to_many_joins'
      end

      def count_records
        load_target.size
      end

      def invertible_for?(_record)
        false
      end
    end
  end
end
