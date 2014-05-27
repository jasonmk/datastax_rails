module DatastaxRails#:nodoc:
  module Cql #:nodoc:
    class Select < Base #:nodoc:
      def initialize(klass, select)
        @klass = klass
        @select = select.join(",")
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
      
      def using(consistency)
        @consistency = consistency
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
        
        @conditions.each do |k,v|
          if v.kind_of?(Array)
            conditions << "\"#{k.to_s}\" IN (#{('?'*v.size).split(//).join(',')})"
            @values += v
          else
            conditions << "\"#{k.to_s}\" = ?"
            @values << v
          end
        end
        
        unless conditions.empty?
          stmt << "WHERE #{conditions.join(" AND ")} "
        end
        
        if @order
          stmt << "ORDER BY #{@order} "
        end
        
        if @limit
          stmt << "LIMIT #{@limit} "
        end
        
        if @allow_filtering
          stmt << "ALLOW FILTERING "
        end
        
        stmt
      end
    end
  end
end