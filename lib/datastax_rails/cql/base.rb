module DatastaxRails
  module Cql
    class Base
      def to_cql #:nodoc:
        nil
      end
      
      def execute
        cql = self.to_cql
        puts cql
        Rails.logger.debug(cql)
        DatastaxRails::Base.connection.execute_cql_query(cql)
      end
    end
  end
end