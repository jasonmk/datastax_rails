module DatastaxRails
  module Schema
    # DatastaxRails reads the attributes from the individual models. This class migrates both Cassandra
    # and Solr to the point where they reflect what is specified in the models.
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
        say_with_time('Migrating all models') do
          FileList[rails_models].each do |model|
            require model
          end

          count = 0
          DatastaxRails::Base.models.each do |m|
            count += migrate_one(m, force) unless m.abstract_class?
          end
          count
        end
      end

      def migrate_one(model, force = false)
        count = 0
        say_with_time("Migrating #{model.name} to latest version") do
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

      # Determine all models to be included within the migration
      # using Rails config paths instead of absolute paths.
      # This enables Rails Engines to monkey patch their own
      # models in, to be automatically included within migrations.
      #
      # @see http://pivotallabs.com/leave-your-migrations-in-your-rails-engines/
      #
      # @return [Array] list of configured application models
      def rails_models
        Rails.configuration.paths['app/models'].expanded.map { |p| p + '/*.rb' }
      end

      # Checks to ensure that the schema_migrations column family exists and creates it if not
      def check_schema_migrations
        return if column_family_exists?('schema_migrations')
        say 'Creating schema_migrations column family'
        DatastaxRails::Cql::CreateColumnFamily.new('schema_migrations').primary_key('cf')
          .columns(cf: :text, digest: :text, solrconfig: :text, stopwords: :text).execute
      end

      def write(text = '')
        puts(text) if verbose
      end

      def say(message, subitem = false)
        write "#{subitem ? '   ->' : '--'} #{message}"
      end

      def say_with_time(message)
        say(message)
        result = nil
        time = Benchmark.measure { result = yield }
        say format('%.4fs', time.real), :subitem
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
