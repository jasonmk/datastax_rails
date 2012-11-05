# configure Rails Envinronment
ENV["RAILS_ENV"] = "test"
require File.expand_path("../dummy/config/environment.rb",  __FILE__)

require 'rspec/rails'

ENGINE_RAILS_ROOT=File.join(File.dirname(__FILE__), '../')

# Requires supporting ruby files with custom matchers and macros, etc,
# in spec/support/ and its subdirectories.
Dir[File.expand_path(File.join(ENGINE_RAILS_ROOT, "spec/support/**/*.rb"))].each {|f| require f }

RSpec.configure do |config|
  config.alias_it_should_behave_like_to :it_has_behavior, 'has behavior:'
  config.before(:each) do
    DatastaxRails::Base.recorded_classes = {}
  end
  
  config.after(:each) do
    DatastaxRails::Base.recorded_classes.keys.each do |klass|
      DatastaxRails::Base.connection.connection.truncate(klass.column_family.to_s)
    end
  end
  
  # config.after(:all) do
    # DatastaxRails::Base.models.each do |m|
      # DatastaxRails::Cql::Truncate.new(m).execute
    # end
  # end
end
