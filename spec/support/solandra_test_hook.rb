require 'active_support'
require 'datastax_rails'
require 'datastax_rails/base'
module DatastaxRails
  class Base
    class_attribute :recorded_classes
    
    def save_with_record_class(*args)
      DatastaxRails::Base.recorded_classes[self.class] = nil
      save_without_record_class(*args)
    end
    alias_method_chain :save, :record_class
  end
end