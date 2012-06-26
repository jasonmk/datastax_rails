module DatastaxRails #:nodoc:
  module Identity
    extend ActiveSupport::Concern
    extend ActiveSupport::Autoload

    autoload :Key
    autoload :AbstractKeyFactory
    autoload :UUIDKeyFactory
    autoload :NaturalKeyFactory
    autoload :HashedNaturalKeyFactory
    autoload :CustomKeyFactory

    module ClassMethods
      # Indicate what kind of key the model will have: uuid or natural
      #
      # @param [:uuid, :natural] the type of key
      # @param the options you want to pass along to the key factory (like :attributes => :name, for a natural key).
      # 
      def key(name_or_factory = :uuid, *options)
        @key_factory = case name_or_factory
        when :uuid
          UUIDKeyFactory.new
        when :natural
          NaturalKeyFactory.new(*options)
        when :custom
          CustomKeyFactory.new(*options)
        else
          name_or_factory
        end
      end

      def next_key(object = nil)
        @key_factory.next_key(object).tap do |key|
          raise "Keys may not be nil" if key.nil?
        end
      end

      def parse_key(string)
        @key_factory.parse(string)
      end
    end

    def id
      key.to_s
    end

    # TODO test this
    def id=(key)
      self.key = self.class.parse_key(key)
      id
    end
  end
end
