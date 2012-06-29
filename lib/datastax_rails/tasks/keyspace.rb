module DatastaxRails
  module Tasks
    class Keyspace
      class << self
        def exists?(name)
          connection.keyspaces.collect(&:name).include? name.to_s
        end
  
        def create(name, options = {})
          opts = { :name => name.to_s,
                   :strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy'}.merge(options)
  
          if(exists?(name.to_s))
            puts "Keyspace #{name.to_s} already exists"
            return false
          else
            cql = DatastaxRails::Cql::CreateKeyspace.new(opts.delete(:name))
            cql.strategy_class(opts.delete(:strategy_class))
            debugger
            strategy_options = opts.delete('strategy_options')
            cql.strategy_options(strategy_options.symbolize_keys)
            puts cql.to_cql
            connection.execute_cql_query(cql.to_cql)
            return true
          end
        end
  
        def drop(name)
          connection.execute_cql_query(DatastaxRails::Cql::DropKeyspace.new(name.to_s).to_cql)
        end
  
        def set(name)
          connection.execute_cql_query(DatastaxRails::Cql::UseKeyspace.new(name.to_s).to_cql)
        end
  
        def get
          connection.keyspace
        end
  
        def clear
          return puts 'Cannot clear system keyspace' if connection.keyspace == 'system'
  
          connection.clear_keyspace!
        end
  
        private
  
          def connection
            unless @connection
              config = YAML.load_file(Rails.root.join("config", "datastax.yml"))[Rails.env]
              @connection = CassandraCQL::Database.new(config["servers"], :keyspace => 'system')
            end
            @connection
          end
      end
    end
  end
end
