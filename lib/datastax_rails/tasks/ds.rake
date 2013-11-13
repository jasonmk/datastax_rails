namespace :ds do
  task :configure => :environment do
    @configs = YAML.load_file(Rails.root.join("config", "datastax.yml"))
    @config = @configs[Rails.env || 'development']
    @migrator = DatastaxRails::Schema::Migrator.new(@config['keyspace'])
  end

  desc 'Create the keyspace in config/datastax.yml for the current environment'
  task :create do
    @configs = YAML.load_file(Rails.root.join("config", "datastax.yml"))
    @config = @configs[Rails.env || 'development']
    DatastaxRails::Base.establish_connection(@config.with_indifferent_access.merge(:keyspace => 'system'))
    DatastaxRails::Schema::Migrator.new('system').create_keyspace(@config['keyspace'], @config)
  end

  desc 'Drop keyspace in config/datastax.yml for the current environment'
  task :drop => :configure do
    @migrator.drop_keyspace
  end

  desc 'Migrate keyspace to latest version -- pass in model name to force an upload of just that one (all force-uploads everything).'
  task :migrate, [:force_cf] => :configure do |t, args|
    if args[:force_cf].blank?
      @migrator.migrate_all
    else
      args[:force_cf] == 'all' ? @migrator.migrate_all(true) : @migrator.migrate_one(args[:force_cf].constantize, true)
    end
  end
  
  desc 'Alias for ds:migrate to maintain backwards-compatibility'
  task :schema, [:force_cf] => :migrate
  
  desc 'Rebuild SOLR Index -- pass in a model name (all rebuilds everything)'
  task :reindex, [:model] => :configure do |t, args|
    if args[:model].blank?
      puts "\nUSAGE: rake ds:reindex[Model]"
    else
      @migrator.reindex_solr(args[:model].constantize)
    end
  end
  
  desc 'Create SOLR Core (Normally not needed) -- pass in a model name (all creates everything)'
  task :create_core, [:model] => :configure do |t, args|
    if args[:model].blank?
      puts "\nUSAGE: rake ds:create_core[Model]"
    else
      @migrator.create_solr_core(args[:model].constantize)
    end
  end
  
  desc 'Load the seed data from ks/seeds.rb'
  task :seed => :environment do
    seed_file = Rails.root.join("ks","seeds.rb")
    load(seed_file) if seed_file.exist?
  end
end

