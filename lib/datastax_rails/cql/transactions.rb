module DatastaxRails
  module Cql
    module Transactions
      extend ActiveSupport::Concern

      included do
        alias_method_chain :to_cql, :transactions
      end

      def initialize(*)
        @if_conditions = {}
        @if_exists = nil
        @if_not_exists = nil
        super
      end

      def iff(columns)
        @if_conditions = columns
        self
      end

      def if_exists
        @if_exists = true
        self
      end

      def if_not_exists
        @if_not_exists = true
        self
      end

      def to_cql_with_transactions
        stmt = to_cql_without_transactions
        if @if_not_exists
          stmt << ' IF NOT EXISTS'
        elsif @if_exists
          stmt << ' IF EXISTS'
        elsif @if_conditions.present?
          conditions = []
          @if_conditions.each do |k, v|
            conditions << "\"#{k}\" = ?"
            @values << v
          end
          stmt << " IF #{conditions.join(' AND ')}"
        end
        stmt
      end
    end
  end
end
