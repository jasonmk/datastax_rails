namespace :ks do
  task :configure => :environment do
    @configs = YAML.load_file(Rails.root.join("config", "cassandra.yml"))
    @config = @configs[Rails.env || 'development']
  end

  #task :set_keyspace => :configure do
    #set_keyspace
  #end

  desc 'Create the keyspace in config/cassandra.yml for the current environment'
  task :create => :configure do
    DatastaxRails::Tasks::Keyspace.new.create @config['keyspace'], @config
    puts "Created keyspace: #{@config['keyspace']}"
  end

  namespace :create do
    desc 'Create keyspaces in config/cassandra.yml for all environments'
    task :all => :configure do
      created = []
      @configs.values.each do |config|
        DatastaxRails::Tasks::Keyspace.new.create config['keyspace'], config
        created << config['keyspace']
      end
      puts "Created keyspaces: #{created.join(', ')}"
    end
  end

  desc 'Drop keyspace in config/cassandra.yml for the current environment'
  task :drop => :configure do
    DatastaxRails::Tasks::Keyspace.new.drop @config['keyspace']
    puts "Dropped keyspace: #{@config['keyspace']}"
  end

  namespace :drop do
    desc 'Drop keyspaces in config/cassandra.yml for all environments'
    task :all => :configure do
      dropped = []
      @configs.values.each do |config|
        DatastaxRails::Tasks::Keyspace.new.drop config['keyspace']
        dropped << config['keyspace']
      end
      puts "Dropped keyspaces: #{dropped.join(', ')}"
    end
  end

  desc 'Migrate the keyspace (options: VERSION=x)'
  task :migrate => :configure do
    version = ( ENV['VERSION'] ? ENV['VERSION'].to_i : nil )
    DatastaxRails::Schema::Migrator.migrate SolandraObject::Schema::Migrator.migrations_path, version
    schema_dump
  end

  desc 'Rolls the schema back to the previous version (specify steps w/ STEP=n)'
  task :rollback => :set_keyspace do
    step = ENV['STEP'] ? ENV['STEP'].to_i : 1
    DatastaxRails::Schema::Migrator.rollback SolandraObject::Schema::Migrator.migrations_path, step
    schema_dump
  end

  desc 'Pushes the schema to the next version (specify steps w/ STEP=n)'
  task :forward => :set_keyspace do
    step = ENV['STEP'] ? ENV['STEP'].to_i : 1
    DatastaxRails::Schema::Migrator.forward SolandraObject::Schema::Migrator.migrations_path, step
    schema_dump
  end

  namespace :schema do
    desc 'Create ks/schema.json file that can be portably used against any Cassandra instance supported by DatastaxRails'
    task :dump => :configure do
      schema_dump
    end

    desc 'Load ks/schema.json file into Cassandra'
    task :load => :configure do
      schema_load
    end
  end

  namespace :test do
    desc 'Load the development schema in to the test keyspace'
    task :prepare => :configure do
      schema_dump :development
      schema_load :test
    end
  end

  desc 'Retrieves the current schema version number'
  task :version => :set_keyspace do
    version = DatastaxRails::Schema::Migrator.current_version
    puts "Current version: #{version}"
  end
  
  desc 'Load the seed data from ks/seeds.rb'
  task :seed => :environment do
    seed_file = Rails.root.join("ks","seeds.rb")
    load(seed_file) if seed_file.exist?
  end

  private

  def schema_dump(env = Rails.env)
    ks = set_keyspace env
    File.open "#{Rails.root}/ks/schema.json", 'w' do |file|
      schema = ActiveSupport::JSON.decode(ks.schema_dump.to_json)
      JSON.pretty_generate(schema).split(/\n/).each do |line|
        file.puts line
      end
    end
  end

  def schema_load(env = Rails.env)
    ks = set_keyspace env
    File.open "#{Rails.root}/ks/schema.json", 'r' do |file|
      hash = JSON.parse(file.read(nil))
      ks.schema_load DatastaxRails::Tasks::Keyspace.parse(hash)
    end
  end

  def set_keyspace(env = Rails.env)
    config = @configs[env.to_s || 'development']
    ks = DatastaxRails::Tasks::Keyspace.new
    ks.set config['keyspace']
    ks
  end
end

