module DatastaxRails
  module Cql
    # CQL generation for SELECT
    class Select < Base
      def initialize(klass, select)
        @select = select.join(',')
        @limit = nil
        @conditions = {}
        @order = nil
        @paginate = nil
        @allow_filtering = nil
        super
      end

      def allow_filtering
        @allow_filtering = true
        self
      end

      def paginate(start)
        @paginate = start
        self
      end

      def conditions(conditions)
        @conditions.merge!(conditions)
        self
      end

      def limit(limit)
        @limit = limit
        self
      end

      def order(order)
        @order = order
        self
      end

      def to_cql
        conditions = []
        stmt = "SELECT #{@select} FROM #{@klass.column_family} "

        if @paginate
          conditions << "token(#{@klass.primary_key}) > token(?)"
          @values << @paginate
        end

        @conditions.each do |k, v|
          if k.to_s == @klass.primary_key && v.is_a?(Array)
            conditions << "\"#{k}\" IN (#{('?' * v.size).split(//).join(',')})"
            @values += v
          else
            v = v.first if v.is_a?(Array)
            conditions << "\"#{k}\" = ?"
            @values << v
          end
        end

        stmt << "WHERE #{conditions.join(' AND ')} " unless conditions.empty?
        stmt << "ORDER BY #{@order} " if @order
        stmt << "LIMIT #{@limit} " if @limit
        stmt << 'ALLOW FILTERING ' if @allow_filtering
        stmt
      end
    end
  end
end
