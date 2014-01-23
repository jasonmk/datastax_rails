module DatastaxRails
  module Cql
    class Base
      # Base initialize that sets the default consistency.
      def initialize(klass, *args)
        @consistency = klass.default_consistency.to_s.downcase.to_sym
        @keyspace = DatastaxRails::Base.config[:keyspace]
        @values = []
      end
      
      def using(consistency)
        @consistency = consistency.to_s.downcase.to_sym
        self
      end
      
      def key_name
        @klass.key_factory.key_columns
      end

      # Abstract.  Should be overridden by subclasses
      def to_cql
        raise NotImplementedError
      end
      
      # Generates the CQL and calls Cassandra to execute it.
      # If you are using this outside of Rails, then DatastaxRails::Base.connection must have
      # already been set up (Rails does this for you).
      def execute
        cql = self.to_cql
        puts cql if ENV['DEBUG_CQL'] == 'true'
        if(@values.empty?)
          DatastaxRails::Base.connection.execute(cql, :consistency => @consistency)
        else
          stmt = DatastaxRails::Base.connection.prepare(cql)
          stmt.execute(*@values, :consistency => @consistency)
        end
      end
      
      def escape(str)
        str.gsub("'", "''")
      end

      def quote(obj)
        if obj.kind_of?(Array)
          obj.map { |member| quote(member) }.join(",")
        elsif obj.kind_of?(Hash)
          "{"+obj.map{ |key,val| "#{quote(cast_to_cql(key))}:#{quote(cast_to_cql(val))}" }.join(',')+"}"
        elsif obj.kind_of?(String)
          "'" + obj + "'"
        elsif obj.kind_of?(Numeric)
          obj.to_s
        elsif obj.kind_of?(SimpleUUID::UUID)
          obj.to_guid
        elsif obj.kind_of?(TrueClass) or obj.kind_of?(FalseClass)
          obj.to_s
        else
          raise Error::UnescapableObject, "Unable to escape object of class #{obj.class}"
        end
      end

      def cast_to_cql(obj)
        if obj.kind_of?(Array)
          obj.map { |member| cast_to_cql(member) }
        elsif obj.kind_of?(Hash)
          obj
        elsif obj.kind_of?(Numeric)
          obj
        elsif obj.kind_of?(Date)
          obj.strftime('%Y-%m-%d')
        elsif obj.kind_of?(Time)
          (obj.to_f * 1000).to_i
        elsif obj.kind_of?(SimpleUUID::UUID)
          obj
        elsif obj.kind_of?(TrueClass) or obj.kind_of?(FalseClass)
          obj
        # There are corner cases where this is an invalid assumption but they are extremely rare.
        # The alternative is to make the user pack the data on their own .. let's not do that until we have to
        elsif obj.kind_of?(String) and Utility.binary_data?(obj)
          escape(obj.unpack('H*')[0])
        else
          RUBY_VERSION >= "1.9" ? escape(obj.to_s.dup.force_encoding('ASCII-8BIT')) : escape(obj.to_s.dup)
        end
      end

      def sanitize(statement, bind_vars=[])
        # If there are no bind variables, return the statement unaltered
        return statement if bind_vars.empty?

        bind_vars = bind_vars.dup
        expected_bind_vars = statement.count("?")

        raise Error::InvalidBindVariable, "Wrong number of bound variables (statement expected #{expected_bind_vars}, was #{bind_vars.size})" if expected_bind_vars != bind_vars.size

        statement.gsub(/\?/) {
          quote(cast_to_cql(bind_vars.shift))
        }
      end
    end
  end
end