namespace :ds do
  task :configure => :environment do
    @configs = YAML.load_file(Rails.root.join("config", "datastax.yml"))
    @config = @configs[Rails.env || 'development']
  end

  desc 'Create the keyspace in config/datastax.yml for the current environment'
  task :create do
    @configs = YAML.load_file(Rails.root.join("config", "datastax.yml"))
    @config = @configs[Rails.env || 'development']
    ret = DatastaxRails::Tasks::Keyspace.create @config['keyspace'], @config
    puts "Created keyspace: #{@config['keyspace']}" if ret
  end

  namespace :create do
    desc 'Create keyspaces in config/datastax.yml for all environments'
    task :all => :configure do
      created = []
      @configs.values.each do |config|
        DatastaxRails::Tasks::Keyspace.create config['keyspace'], config
        created << config['keyspace']
      end
      puts "Created keyspaces: #{created.join(', ')}"
    end
  end

  desc 'Drop keyspace in config/datastax.yml for the current environment'
  task :drop => :configure do
    DatastaxRails::Tasks::Keyspace.drop @config['keyspace']
    puts "Dropped keyspace: #{@config['keyspace']}"
  end

  namespace :drop do
    desc 'Drop keyspaces in config/datastax.yml for all environments'
    task :all => :configure do
      dropped = []
      @configs.values.each do |config|
        DatastaxRails::Tasks::Keyspace.drop config['keyspace']
        dropped << config['keyspace']
      end
      puts "Dropped keyspaces: #{dropped.join(', ')}"
    end
  end
  
  desc 'Upload SOLR schemas -- pass in model name to force an upload (:all uploads everything).'
  task :schema, [:force_cf] => :configure do |t, args|
    cf = DatastaxRails::Tasks::ColumnFamily.new(@config['keyspace'])
    cf.upload_solr_schemas(args[:force_cf])
  end
  
  desc 'Rebuild SOLR Index -- pass in a model name (:all rebuilds everything)'
  task :reindex, [:model] => :configure do |t, args|
    if args[:model].blank?
      puts "\nUSAGE: rake ds:reindex[Model]"
    else
      cf = DatastaxRails::Tasks::ColumnFamily.new(@config['keyspace'])
      puts "Reindexing #{args[:model]}"
      cf.reindex_solr(args[:model])
      puts "Reindexing will run in the background"
    end
  end
  
  desc 'Load the seed data from ds/seeds.rb'
  task :seed => :environment do
    seed_file = Rails.root.join("ks","seeds.rb")
    load(seed_file) if seed_file.exist?
  end
end

