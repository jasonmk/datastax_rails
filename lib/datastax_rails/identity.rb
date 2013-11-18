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
      attr_accessor :key_factory
      # Indicate what kind of key the model will have: uuid or natural
      #
      # @param [:uuid, :natural] name_or_factory the type of key
      # @param [Hash] options the options you want to pass along to the key factory (like :attributes => :name, for a natural key).
      # 
      def key(name_or_factory = :uuid, *options)
        @key_factory = case name_or_factory
        when :uuid
          UUIDKeyFactory.new(*options)
        when :natural
          NaturalKeyFactory.new(*options)
        when :custom
          CustomKeyFactory.new(*options)
        else
          name_or_factory
        end
      end

      # The next key for the given object. Delegates the actual work to the factory which may
      # or may not use the passed in object to generate the key.
      #
      # @param [DatastaxRails::Base] object the object for which the key is being generated
      # @return [String] a key for this object
      def next_key(object = nil)
        @key_factory.next_key(object).tap do |key|
          raise "Keys may not be nil" if key.nil?
        end
      end

      # Parses out a key from the given string. Delegates the actual work to the factory.
      # Return type varies depending on what type of key is used.
      #
      # @param [String] string a string representing a primary key
      # @return an object representing the same key
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
