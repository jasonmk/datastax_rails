module DatastaxRails
  module Types
    class DirtySet < Set
      include DirtyCollection

      methods = [:delete, :<<, :add, :clear, :subtract] +
                Set.instance_methods(true).select{|m| m.to_s.ends_with?('!')}
      
      methods.each do |m|
        define_method(m) do |*args, &block|
          modifying do
            super(*args, &block)
          end
        end
      end
    end
  end
end