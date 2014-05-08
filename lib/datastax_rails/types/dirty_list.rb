module DatastaxRails
  module Types
    class DirtyList < Array
      include DirtyCollection

      methods = [:<<, :delete, :[]=, :push, :pop, :unshift, :shift, :insert] +
                Array.instance_methods(true).select{|m| m.to_s.ends_with?('!')}
      
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