namespace :dsr do
  task :configure => :environment do
    @configs = YAML.load_file(Rails.root.join("config", "datastax.yml"))
    @config = @configs[Rails.env || 'development']
  end

  desc 'Create the keyspace in config/cassandra.yml for the current environment'
  task :create do
    @configs = YAML.load_file(Rails.root.join("config", "datastax.yml"))
    @config = @configs[Rails.env || 'development']
    DatastaxRails::Tasks::Keyspace.create @config['keyspace'], @config
    puts "Created keyspace: #{@config['keyspace']}"
  end

  namespace :create do
    desc 'Create keyspaces in config/cassandra.yml for all environments'
    task :all => :configure do
      created = []
      @configs.values.each do |config|
        DatastaxRails::Tasks::Keyspace.create config['keyspace'], config
        created << config['keyspace']
      end
      puts "Created keyspaces: #{created.join(', ')}"
    end
  end

  desc 'Drop keyspace in config/cassandra.yml for the current environment'
  task :drop => :configure do
    DatastaxRails::Tasks::Keyspace.drop @config['keyspace']
    puts "Dropped keyspace: #{@config['keyspace']}"
  end

  namespace :drop do
    desc 'Drop keyspaces in config/cassandra.yml for all environments'
    task :all => :configure do
      dropped = []
      @configs.values.each do |config|
        DatastaxRails::Tasks::Keyspace.drop config['keyspace']
        dropped << config['keyspace']
      end
      puts "Dropped keyspaces: #{dropped.join(', ')}"
    end
  end
  
  task :schema => :configure do
    cf = DatastaxRails::Tasks::ColumnFamily.new(@config['keyspace'])
    cf.upload_solr_schemas
  end

  desc 'Migrate the keyspace (options: VERSION=x)'
  task :migrate => :configure do
    version = ( ENV['VERSION'] ? ENV['VERSION'].to_i : nil )
    DatastaxRails::Schema::Migrator.migrate DatastaxRails::Schema::Migrator.migrations_path, version
    schema_dump
  end

  desc 'Load the seed data from ks/seeds.rb'
  task :seed => :environment do
    seed_file = Rails.root.join("ks","seeds.rb")
    load(seed_file) if seed_file.exist?
  end
end

