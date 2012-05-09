require 'digest/sha1'
module DatastaxRails
  module Identity
    class HashedNaturalKeyFactory < NaturalKeyFactory
      def next_key(object)
        NaturalKey.new(Digest::SHA1.hexdigest(attributes.map { |a| object.attributes[a.to_s] }.join(separator)))
      end
    end
  end
end
