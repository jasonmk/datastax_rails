module DatastaxRails
  module Identity
    # Key factories need to support 3 operations
    class AbstractKeyFactory
      attr_accessor :key_columns
      # Next key takes an object and returns the key object it should use.
      # object will be ignored with synthetic keys but could be useful with natural ones
      #
      # @abstract
      # @param  [DatastaxRails::Base] object the object that needs a new key
      # @return [DatastaxRails::Identity::Key] the key
      #
      def next_key(object)
        raise NotImplementedError, "#{self.class.name}#next_key isn't implemented."
      end

      # Parse should create a new key object from the 'to_param' format
      #
      # @abstract
      # @param  [String] string the result of calling key.to_param
      # @return [DatastaxRails::Identity::Key] the parsed key
      #
      def parse(string)
        raise NotImplementedError, "#{self.class.name}#parse isn't implemented."
      end
    end
  end
end

