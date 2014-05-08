module DatastaxRails
  module Types
    class DirtyMap < ActiveSupport::HashWithIndifferentAccess
      include DirtyCollection

      methods = [:delete, :[]=, :store] +
                ActiveSupport::HashWithIndifferentAccess.instance_methods(true).select{|m| m.to_s.ends_with?('!')}
      
      methods.each do |m|
        original_method = ActiveSupport::HashWithIndifferentAccess.instance_method(m)
        define_method(m) do |*args, &block|
          modifying do
            original_method.bind(self).call(*args, &block)
          end
        end
      end
      
      def dup
        self.class.new(record, name, self, options).tap do |new_hash|
          new_hash.default = default
        end
      end
    end
  end
end