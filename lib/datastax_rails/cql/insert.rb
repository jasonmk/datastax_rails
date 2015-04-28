module DatastaxRails
  module Cql
    # CQL generation for INSERT
    class Insert < Base
      def initialize(klass)
        @klass = klass
        @ttl = nil
        @timestamp = nil
        @columns = {}
        super
      end

      def columns(columns)
        @columns.merge!(columns)
        self
      end

      def ttl(ttl)
        @ttl = ttl
        self
      end

      def timestamp(timestamp)
        @timestamp = timestamp
        self
      end

      def to_cql
        keys = []
        @columns.each do |k, v|
          keys << k.to_s
          @values << v
        end
        stmt =  "INSERT INTO #{@klass.column_family} (#{keys.join(',')}) "
        stmt << "VALUES (#{('?' * keys.size).split(//).join(',')}) "
        if @ttl || @timestamp
          stmt << 'USING '
          stmt << "TTL #{@ttl} " if @ttl
          stmt << 'AND ' if @ttl && @timestamp
          stmt << "TIMESTAMP #{@timestamp} " if @timestamp
        end
        stmt.force_encoding('UTF-8')
      end
    end
  end
end
