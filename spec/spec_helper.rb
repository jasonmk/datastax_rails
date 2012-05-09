# configure Rails Envinronment
ENV["RAILS_ENV"] = "test"
require File.expand_path("../dummy/config/environment.rb",  __FILE__)

require 'rspec/rails'

ENGINE_RAILS_ROOT=File.join(File.dirname(__FILE__), '../')

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[File.join(ENGINE_RAILS_ROOT, "spec/support/**/*.rb")].each {|f| require f }

RSpec.configure do |config|
  config.before(:each) do
    DatastaxRails::Base.recorded_classes = {}
    Sunspot.remove_all!
    Sunspot.commit
  end
  
  config.after(:each) do
    DatastaxRails::Base.recorded_classes.keys.each do |klass|
      DatastaxRails::Base.connection.truncate!(klass.column_family.to_sym)
    end
  end
end
