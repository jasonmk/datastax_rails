module DatastaxRails
  module Batches
    # Yields each record that was found by the find +options+. The find is
    # performed by find_in_batches with a batch size of 1000 (or as
    # specified by the <tt>:batch_size</tt> option).
    #
    # Example:
    #
    #   Person.where("age > 21").find_each do |person|
    #     person.party_all_night!
    #   end
    #
    # Note: This method is only intended to use for batch processing of
    # large amounts of records that wouldn't fit in memory all at once. If
    # you just need to loop over less than 1000 records, it's probably
    # better just to use the regular find methods.
    #
    # @param options [Hash] finder options
    # @yield [record] a single DatastaxRails record
    def find_each(options = {})
      find_in_batches(options) do |records|
        records.each { |record| yield record }
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
    # It's not possible to set the order. That is automatically set according
    # Cassandra's key placement strategy. Records are retrieved and returned
    # using only Cassandra and no SOLR interaction. This also mean that this
    # method only works with any type of primary key (unlike ActiveRecord).
    # You can't set the limit, however. That's used to control the batch sizes.
    #
    # Example:
    #
    #   Person.where("age > 21").find_in_batches do |group|
    #     sleep(50) # Make sure it doesn't get too crowded in there!
    #     group.each { |person| person.party_all_night! }
    #   end
    #
    # @param options [Hash] finder options
    # @yeild [records] a batch of DatastaxRails records
    def find_in_batches(options = {})
      relation = self.with_cassandra

      unless @order_values.empty? && @per_page_value.blank?
        DatastaxRails::Base.logger.warn("Scoped order and limit are ignored, it's forced to be batch order and batch size")
      end

      if (finder_options = options.except(:start, :batch_size)).present?
        raise "You can't specify an order, it's forced to be #{batch_order}" if options[:order].present?
        raise "You can't specify a limit, it's forced to be the batch_size" if options[:limit].present?

        relation = apply_finder_options(finder_options)
      end

      start = options.delete(:start)
      batch_size = options.delete(:batch_size) || 1000

      relation = relation.limit(batch_size)
      records = start ? relation.where(:KEY).greater_than(start).to_a : relation.to_a

      while records.size > 0
        records_size = records.size
        primary_key_offset = records.last.id
        yield records

        break if records_size < batch_size

        if primary_key_offset
          records = relation.where(:KEY).greater_than(primary_key_offset).to_a
        else
          raise "Primary key not included in the custom select clause"
        end
      end
    end
  end
end