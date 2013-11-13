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
          if(definition.coder.options[:indexed] == :cassandra)
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
          end
        end
        count
      end
      
      # Creates a payload column family via CQL
      def create_payload_column_family(model)
        say "Creating Payload Column Family", :subitem
        columns = {:chunk => :int, :payload => :text}
        DatastaxRails::Cql::CreateColumnFamily.new(model.column_family).key_name(:digest).key_columns("digest, chunk").key_type(:text).columns(columns).with("COMPACT STORAGE").execute
      end
      
      # Creates a wide-storage column family via CQL
      def create_wide_storage_column_family(model)
        say "Creating Wide-Storage Column Family", :subitem
        key_name = model.key_factory.attributes.join
        cluster_by = model.cluster_by.keys.first
        cluster_dir = model.cluster_by.values.first
        key_columns = "#{key_name}, #{cluster_by}"
        columns = {}
        model.attribute_definitions.each {|k,v| columns[k] = v.coder.options[:cassandra_type] unless k.to_s == key_name}
        DatastaxRails::Cql::CreateColumnFamily.new(model.column_family).key_name(key_name).key_columns(key_columns).key_type(:text).columns(columns).
          with("CLUSTERING ORDER BY (#{cluster_by} #{cluster_dir.to_s.upcase})").execute
      end
      
      # Creates the named keyspace
      def create_keyspace(keyspace, options = {})
        opts = { :name => keyspace.to_s,
                 :strategy_class => 'org.apache.cassandra.locator.NetworkTopologyStrategy'}.with_indifferent_access.merge(options)

        if(connection.keyspaces.collect(&:name).include?(keyspace.to_s))
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
      
      # Checks the Cassandra system tables to see if the key column is named properly.  This is
      # a migration method to handle the fact that Solr used to create column families with "KEY"
      # instead of the now default "key".
      def check_key_name(cf)
        count = 0
        if(cf.respond_to?(:column_family))
          cf = cf.column_family
        end
        klass = OpenStruct.new(:column_family => 'system.schema_columnfamilies', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = CassandraCQL::Result.new(cql.select("key_alias, key_aliases").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf).execute)
        result = results.fetch
        if(result && (result['key_alias'] == 'KEY' || result['key_aliases'].include?('KEY')) && (result['key_aliases'].blank? || !result['key_aliases'].include?('key')))
          count += 1
          say "Renaming KEY column", :subitem
          DatastaxRails::Cql::AlterColumnFamily.new(cf).rename("KEY",'key').execute
        end
        count
      end
      
      # Computes the expected solr index name as reported by CQL.
      def solr_index_cql_name(cf, column)
        "#{@keyspace}_#{cf.to_s}_#{column.to_s}_index"
      end
      
      # Computes the expected cassandra index name as reported by CQL.
      def cassandra_index_cql_name(cf, column)
        "#{cf.to_s}_#{column.to_s}_idx"
      end
      
      # Checks the Cassandra system tables to see if a column family exists
      def column_family_exists?(cf)
        klass = OpenStruct.new(:column_family => 'system.schema_columnfamilies', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = CassandraCQL::Result.new(cql.select("count(*)").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf).execute)
        results.fetch['count'] > 0
      end
      
      # Checks the Cassandra system tables to see if a column exists on a column family
      def column_exists?(cf, col)
        klass = OpenStruct.new(:column_family => 'system.schema_columns', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = CassandraCQL::Result.new(cql.select("count(*)").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf, 'column_name' => col).execute)
        exists = results.fetch['count'] > 0
        unless exists
          # We need to check if it's part of the primary key (ugh)
          klass = OpenStruct.new(:column_family => 'system.schema_columnfamilies', :default_consistency => 'QUORUM')
          cql = DatastaxRails::Cql::ColumnFamily.new(klass)
          results = CassandraCQL::Result.new(cql.select("column_aliases, key_aliases").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf).execute)
          row = results.fetch
          exists = row['key_aliases'].include?(col.to_s) || row['column_aliases'].include?(col.to_s)
        end
        exists
      end
      
      # Checks the Cassandra system tables to see if an index exists on a column family
      def index_exists?(cf, col)
        klass = OpenStruct.new(:column_family => 'system.schema_columns', :default_consistency => 'QUORUM')
        cql = DatastaxRails::Cql::ColumnFamily.new(klass)
        results = CassandraCQL::Result.new(cql.select("index_name").conditions('keyspace_name' => @keyspace, 'columnfamily_name' => cf, 'column_name' => col).execute)
        results.fetch['index_name'] != nil
      end
    end
  end
end