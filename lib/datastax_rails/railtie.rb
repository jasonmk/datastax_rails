require 'rubygems'
require 'datastax_rails'
require 'rails'
require 'action_controller/railtie'

module DatastaxRails
  class Railtie < Rails::Railtie
    config.action_dispatch.rescue_responses.merge!(
      'DatastaxRails::RecordNotFound' => :not_found,
      'DatastaxRails::RecordInvalid'  => :unprocessable_entity,
      'DatastaxRails::RecordNotSaved' => :unprocessable_entity)
    
    initializer 'datastax_rails.init' do
      ActiveSupport.on_load(:datastax_rails) do
      end
      config = YAML.load_file(Rails.root.join("config", "datastax.yml"))
      DatastaxRails::Base.establish_connection(config[Rails.env].with_indifferent_access)
    end
    
    rake_tasks do
      load 'datastax_rails/tasks/ds.rake'
    end
    
    # generators do
      # require 'datastax_rails/generators/migration_generator'
    # end
  end
end
