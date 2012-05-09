module DatastaxRails
  module Tasks
    class Keyspace
      def self.parse(hash)
        ks = Cassandra::Keyspace.new.with_fields hash
        ks.cf_defs = []
        hash['cf_defs'].each do |cf|
          ks.cf_defs << Cassandra::ColumnFamily.new.with_fields(cf)
        end
        ks
      end

      def exists?(name)
        connection.keyspaces.collect(&:name).include? name.to_s
      end

      def create(name, options = {})
        opts = { :name => name.to_s,
                 :strategy_class => 'SimpleStrategy',
                 :replication_factor => 1}.merge(options)

        cql = DatastaxRails::Cql::CreateKeyspace(opts.delete(:name))
        cql.strategy_class(opts.delete(:strategy_class))
        cql.strategy_options(opts)
        
        connection.execute_cql_query(cql.to_cql)
      end

      def drop(name)
        connection.execute_cql_query(DatastaxRails::Cql::DropKeyspace(name.to_s).to_cql)
      end

      def set(name)
        connection.execute_cql_query(DatastaxRails::Cql::UseKeyspace(name.to_s).to_cql)
      end

      def get
        connection.keyspace
      end

      def clear
        return puts 'Cannot clear system keyspace' if connection.keyspace == 'system'

        connection.clear_keyspace!
      end

      def schema_dump
        connection.schema
      end

      def schema_load(schema)
        connection.schema.cf_defs.each do |cf|
          connection.drop_column_family cf.name
        end

        keyspace = get
        schema.cf_defs.each do |cf|
          cf.keyspace = keyspace
          connection.add_column_family cf
        end
      end

      private

      def connection
        unless @connection
          config = YAML.load_file(Rails.root.join("config", "cassandra.yml"))
          @connection = CassandraCQL::Database.new(config["servers"], :keyspace => 'system')
        end
        @connection
      end
    end
  end
end
