module DatastaxRails#:nodoc:
  module Cql #:nodoc:
    class Select < Base #:nodoc:
      def initialize(klass, select)
        @klass = klass
        @select = select.join(",")
        @limit = nil
        @conditions = {}
        @order = nil
        super
      end
      
      def using(consistency)
        @consistency = consistency
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
        values = []
        stmt = "SELECT #{@select} FROM #{@klass.column_family} USING CONSISTENCY #{@consistency} "
        @conditions.each do |k,v|
          values << v
          if v.kind_of?(Array)
            conditions << "\"#{k.to_s}\" IN (?)"
          else
            conditions << "\"#{k.to_s}\" = ?"
          end
        end
        
        unless conditions.empty?
          stmt << "WHERE #{conditions.join(" AND ")} "
        end
        
        if @limit
          stmt << "LIMIT #{@limit} "
        end
        
        if @order
          stmt << "ORDER BY #{@order}"
        end
        
        CassandraCQL::Statement.sanitize(stmt, values)
      end
    end
  end
end