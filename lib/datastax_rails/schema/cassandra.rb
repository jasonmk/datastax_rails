module DatastaxRails
  module Schema
    module Cassandra
      # Check for missing columns or columns needing cassandra indexes
      def check_missing_schema(model)
        count = 0
        model.attribute_definitions.each do |attribute, definition|
          unless column_exists?(model.column_family.to_s, attribute.to_s)
            count += 1
            say "Adding column '#{attribute}'", :subitem
            DatastaxRails::Cql::AlterColumnFamily.new(model.column_family).add(attribute => :text).execute
          end
          if(definition.options[:cql_index] && !definition.options[:solr_index])
            unless index_exists?(model.column_family.to_s, attribute.to_s)
              if index_exists?(model.column_family.to_s, attribute.to_s)
                count += 1
                say "Dropping solr index on #{attribute.to_s}", :subitem
                DatastaxRails::Cql::DropIndex.new(solr_index_cql_name(model.column_family.to_s, attribute.to_s)).execute
              end
              count += 1
              say "Creating cassandra index on #{attribute.to_s}", :subitem
              DatastaxRails::Cql::CreateIndex.new(cassandra_index_cql_name(model.column_family.to_s, attribute.to_s)).on(model.column_family.to_s).column(attribute.to_s).execute
            end
          elsif(definition.options[:cql_index])
            unless column_exists?(model.column_family.to_s, "__#{attribute.to_s}")
              # Create and populate the new column
              count += 1
              say "Adding column '__#{attribute}'", :subitem
              DatastaxRails::Cql::AlterColumnFamily.new(model.column_family).add("__#{attribute.to_s}" => definition.cql_type).execute
              say "Populating column '__#{attribute}' (this might take a while)", :subitem
              export = "echo \"copy #{model.column_family.to_s} (key, #{attribute.to_s}) TO 'dsr_export.csv';\" | cqlsh #{model.current_server}"
              import = "echo \"copy #{model.column_family.to_s} (key, __#{attribute.to_s}) FROM 'dsr_export.csv';\" | cqlsh #{model.current_server}"
              if system(export)
                system(import)
              else
                @errors << "Looks like you don't have a working cqlsh command in your path.\nRun the following two commands from a server with cqlsh:\n\n#{export}\n#{import}"
              end
            end
            count += 1
            say "Creating cassandra index on __#{attribute.to_s}", :subitem
            DatastaxRails::Cql::CreateIndex.new(cassandra_index_cql_name(model.column_family.to_s, "__#{attribute.to_s}")).on(model.column_family.to_s).column("__#{attribute.to_s}").execute
          end
        end
        count
      end
      
      # Creates a CQL3 backed column family
      def create_cql3_column_family(model)
        say "Creating Column Family via CQL3", :subitem
        columns = {}
        model.attribute_definitions.each {|k,col| columns[k] = col.cql_type}
        pk = model.primary_key.to_s
        if(model.respond_to?(:cluster_by) && model.cluster_by.present?)
          pk += ", #{model.cluster_by.to_s}"
        end
        cql = DatastaxRails::Cql::CreateColumnFamily.new(model.column_family).primary_key(pk).columns(columns)
        cql.with(model.create_options) if model.create_options
        cql.execute
      end
      
      # Creates the named keyspace
      def create_keyspace(keyspace, options = {})
        opts = { :name => keyspace.to_s,
                 :strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy'}.with_indifferent_access.merge(options)

        if(keyspace_exists?(keyspace.to_s))
          say "Keyspace #{keyspace.to_s} already exists"
          return false
        else
          cql = DatastaxRails::Cql::CreateKeyspace.new(opts.delete(:name))
          cql.strategy_class(opts.delete(:strategy_class))
          strategy_options = opts.delete('strategy_options')
          cql.strategy_options(strategy_options.symbolize_keys)
          say "Creating keyspace #{keyspace.to_s}"
          cql.execute
          return true
        end
      end
      
      def drop_keyspace
        say "Dropping keyspace #{@keyspace.to_s}"
        DatastaxRails::Cql::DropKeyspace.new(@keyspace.to_s).execute
      end
      
      # Computes the expected solr index name as reported by CQL.
      def solr_index_cql_name(cf, column)
        "#{@keyspace}_#{cf.to_s}_#{column.to_s}_index"
      end
      
      # Computes the expected cassandra index name as reported by CQL.
      def cassandra_index_cql_name(cf, column)
        "#{cf.to_s}_#{column.to_s}_idx"
      end
      
      # Checks the Cassandra system tables to see if a keyspace exists
      def keyspace_exists?(keyspace)
        klass = OpenStruct.new(:column_family => 'system.schema_keyspaces', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = cql.select("count(*)").conditions('keyspace_name' => keyspace).execute
        results.first['count'].to_i > 0
      end
      
      # Checks the Cassandra system tables to see if a column family exists
      def column_family_exists?(cf)
        klass = OpenStruct.new(:column_family => 'system.schema_columnfamilies', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = cql.select("count(*)").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf).execute
        results.first['count'] > 0
      end
      
      # Checks the Cassandra system tables to see if a column exists on a column family
      def column_exists?(cf, col)
        klass = OpenStruct.new(:column_family => 'system.schema_columns', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = cql.select("count(*)").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf, 'column_name' => col).execute
        exists = results.first['count'] > 0
        unless exists
          # We need to check if it's part of an alias (ugh)
          klass = OpenStruct.new(:column_family => 'system.schema_columnfamilies', :default_consistency => 'QUORUM')
          cql = DatastaxRails::Cql::ColumnFamily.new(klass)
          results = cql.select("column_aliases, key_aliases, value_alias").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf).execute
          row = results.first
          exists = row['key_aliases'].include?(col.to_s) || row['column_aliases'].include?(col.to_s) || (row['value_alias'] && row['value_alias'].include?(col.to_s))
        end
        exists
      end
      
      # Checks the Cassandra system tables to see if an index exists on a column family
      def index_exists?(cf, col)
        klass = OpenStruct.new(:column_family => 'system.schema_columns', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = cql.select("index_name").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf, 'column_name' => col).execute
        results.first['index_name'] != nil
      end
    end
  end
end