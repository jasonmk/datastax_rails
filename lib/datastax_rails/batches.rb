module DatastaxRails
  module Batches
    extend ActiveSupport::Concern

    module ClassMethods
      def find_each
        connection.each(column_family) do |k, v|
          yield instantiate(k, v)
        end
      end

      def find_in_batches(options = {})
        batch_size = options.delete(:batch_size) || 1000

        batch = []

        find_each do |record|
          batch << record
          if batch.size == batch_size
            yield(batch)
            batch = []
          end
        end

        if batch.size > 0
          yield batch
        end
      end

      def batch(&block)
        connection.batch(&block)
      end
    end
  end
end