module DatastaxRails
  module Batches
    # Yields each record that was found by the find +options+. The find is
    # performed by find_in_batches with a batch size of 1000 (or as
    # specified by the <tt>:batch_size</tt> option).
    #
    # Example:
    #
    #   Person.where(in_college: true).find_each do |person|
    #     person.party_all_night!
    #   end
    #
    # Note: This method is only intended to use for batch processing of
    # large amounts of records that wouldn't fit in memory all at once. If
    # you just need to loop over less than 1000 records, it's probably
    # better just to use the regular find methods.
    #
    # You can also pass the +:start+ option to specify an offset to
    # control the starting point.
    #
    # @param options [Hash] finder options
    # @yield [record] a single DatastaxRails record
    def find_each(options = {})
      find_in_batches(options) do |records|
        records.each { |record| yield record }
      end
    end

    # Same as {find_each} but yields the index as a second parameter.
    def find_each_with_index(options = {})
      idx = 0
      find_in_batches(options) do |records|
        records.each do |record|
          yield record, idx
          idx += 1
        end
      end
    end

    # Yields each batch of records that was found by the find +options+ as
    # an array. The size of each batch is set by the <tt>:batch_size</tt>
    # option; the default is 1000.
    #
    # You can control the starting point for the batch processing by
    # supplying the <tt>:start</tt> option. This is especially useful if you
    # want multiple workers dealing with the same processing queue. You can
    # make worker 1 handle all the records between id 0 and 10,000 and
    # worker 2 handle from 10,000 and beyond (by setting the <tt>:start</tt>
    # option on that worker).
    #
    # It's not possible to set the order. For Cassandra based batching, the
    # order is set according to Cassandra's key placement strategy. For Solr
    # based batching, the order is ascending order of the primary key.
    # You can't set the limit either. That's used to control the batch sizes.
    #
    # Example:
    #
    #   Person.where(in_college: true).find_in_batches do |group|
    #     sleep(50) # Make sure it doesn't get too crowded in there!
    #     group.each { |person| person.party_all_night! }
    #   end
    #
    # @param options [Hash] finder options
    # @yield [records] a batch of DatastaxRails records
    def find_in_batches(options = {})
      relation = self

      unless @order_values.empty?
        DatastaxRails::Base.logger.warn('Scoped order and limit are ignored, ' \
                                        "it's forced to be batch order and batch size")
      end

      if (finder_options = options.except(:start, :batch_size)).present?
        fail "You can't specify an order, it's forced to be #{@klass.primary_key}" if options[:order].present?
        fail "You can't specify a limit, it's forced to be the batch_size" if options[:limit].present?

        relation = apply_finder_options(finder_options)
      end

      start = options.delete(:start)
      batch_size = options.delete(:batch_size) || 1000

      relation = relation.limit(batch_size)
      relation = relation.order(@klass.primary_key) if relation.use_solr_value
      records = start ? relation.where(@klass.primary_key).greater_than(start).to_a : relation.to_a
      while records.size > 0
        records_size = records.size
        offset = records.last.id
        yield records

        break if records_size < batch_size
        if offset
          offset = ::Cql::Uuid.new(offset.value + 1) if relation.use_solr_value
          records = relation.where(@klass.primary_key).greater_than(offset).to_a
        else
          fail 'Batch order not included in the custom select clause'
        end
      end
    end
  end
end
