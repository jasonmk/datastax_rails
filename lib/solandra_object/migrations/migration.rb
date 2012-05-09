module DatastaxRails
  module Migrations
    class Migration
      attr_reader :version
      def initialize(version, block)
        @version = version
        @block = block
      end

      def run(attrs)
        @block.call(attrs)
      end
    end
  end
end