module DatastaxRails
  module Schema
    class Migrator
      include DatastaxRails::Schema::Solr
      include DatastaxRails::Schema::Cassandra
      
      cattr_accessor :verbose
      self.verbose = true
      attr_accessor :errors
      
      def initialize(keyspace)
        @keyspace = keyspace
        check_schema_migrations unless keyspace == 'system'
        @errors = []
      end

      def migrate_all(force = false)
        say_with_time("Migrating all models") do
          # Ensure all models are loaded (necessary for non-production mode)
          Dir[Rails.root.join("app","models",'*.rb').to_s].each do |file|
            require File.basename(file, File.extname(file))
          end
          count = 0
          DatastaxRails::Base.models.each do |m|
            if !m.abstract_class?
              count += migrate_one(m, force)
            end
          end
          count
        end
      end
      
      def migrate_one(model, force = false)
        count = 0
        say_with_time("Migrating #{model.name} to latest version") do
          count += check_key_name(model)
          
          unless column_family_exists?(model.column_family.to_s)
            create_cql3_column_family(model)
            count += 1
          end
          
          count += check_missing_schema(model)
          
          unless model <= DatastaxRails::CassandraOnlyModel
            count += upload_solr_configuration(model, force)
          end
        end
        count
      end

      def connection
        DatastaxRails::Base.connection
      end

      private
      
        # Checks to ensure that the schema_migrations column family exists and creates it if not
        def check_schema_migrations
          unless column_family_exists?('schema_migrations')
            say "Creating schema_migrations column family"
            DatastaxRails::Cql::CreateColumnFamily.new('schema_migrations').key_type(:text).columns(:digest => :text, :solrconfig => :text, :stopwords => :text).execute
          end
          
          check_key_name('schema_migrations')
        end
        
        def write(text="")
          puts(text) if verbose
        end
  
        def say(message, subitem=false)
          write "#{subitem ? "   ->" : "--"} #{message}"
        end
  
        def say_with_time(message)
          say(message)
          result = nil
          time = Benchmark.measure { result = yield }
          say "%.4fs" % time.real, :subitem
          say("#{result} changes", :subitem) if result.is_a?(Integer)
          result
        end
  
        def suppress_messages
          save, self.verbose = verbose, false
          yield
        ensure
          self.verbose = save
        end
    end
  end
end
