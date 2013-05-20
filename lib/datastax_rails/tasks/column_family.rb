require 'digest/sha1'

module DatastaxRails
  module Tasks
    class ColumnFamily
      COMPARATOR_TYPES = [:blob, :ascii, :text, :varint, :bigint, :uuid, :timestamp, :boolean, :float, :doublt, :decimal]

      COLUMN_TYPES = [:standard, :super]

      def initialize(keyspace)
        @keyspace = keyspace
      end

      def exists?(name)
        connection.schema.column_family_names.include?(name)
      end

      def create(name, &block)
        cql = DatastaxRails::Cql::CreateColumnFamily.new(name.to_s)
        cql.comparator_type = 'text'
        cql.column_type = 'Standard'

        block.call cf if block

        connection.execute_cql_query(cql.to_cql)
      end

      def drop(name)
        connection.execute_cql_query(DatastaxRails::Cql::DropColumnFamily.new(name.to_s).to_cql)
      end

      def rename(old_name, new_name)
        raise NotImplementedError, "Renaming of column families is not currently supported"
      end

      def clear(name)
        connection.execute_cql_query(DatastaxRails::Cql::Truncate.new(name.to_s).to_cql)
      end
      
      def generate_solr_schema(model)
        @fields = []
        @copy_fields = []
        @fulltext_fields = []
        model.attribute_definitions.values.each do |attr|
          coder = attr.coder
          if coder.options[:solr_type]
            @fields.push({ :name => attr.name,
                           :type => coder.options[:solr_type].to_s,
                           :indexed => coder.options[:indexed].to_s,
                           :stored => coder.options[:stored].to_s,
                           :multi_valued => coder.options[:multi_valued].to_s })
          end
          if coder.options[:sortable] && coder.options[:tokenized]
            @fields.push({ :name => "sort_" + attr.name,
                           :type => "string",
                           :indexed => true,
                           :stored => false,
                           :multi_valued => false })
            @copy_fields.push({ :source => attr.name, :dest => "sort_" + attr.name }) if (coder.options[:indexed] || coder.options[:stored])
          end
          if coder.options[:fulltext]
            @fulltext_fields << attr.name if (coder.options[:indexed] || coder.options[:stored])
          end
        end
        # Sort the fields so that no matter what order the attributes are arranged into the
        # same schema file gets generated
        @fields.sort! {|a,b| a[:name] <=> b[:name]}
        @copy_fields.sort! {|a,b| a[:source] <=> b[:source]}
        @fulltext_fields.sort!
        
        return ERB.new(File.read(File.join(File.dirname(__FILE__),"..","..","..","config","schema.xml.erb"))).result(binding)
      end
      
      def reindex_solr(model)
        if model == ':all'
          models_to_index = DatastaxRails::Base.models
        else
          models_to_index = [model.constantize]
        end
        
        models_to_index.each do |m|
          next if m.payload_model?
          
          url = "#{DatastaxRails::Base.solr_base_url}/admin/cores?action=RELOAD&name=#{DatastaxRails::Base.config[:keyspace]}.#{m.column_family}&reindex=true&deleteAll=false"
          puts "Posting reindex command to '#{url}'"
          `curl -s -X POST '#{url}'`
          if Rails.env.production?
            sleep(5)
          end
        end
      end
      
      def upload_solr_schemas(column_family)
        force = !column_family.nil?
        column_family ||= :all
        # Ensure schema migrations CF exists
        unless connection.schema.column_families['schema_migrations']
          connection.execute_cql_query(DatastaxRails::Cql::CreateColumnFamily.new('schema_migrations').key_type(:text).columns(:digest => :text, :solrconfig => :text, :stopwords => :text).to_cql)
        end
        
        solrconfig = File.read(File.join(File.dirname(__FILE__),"..","..","..","config","solrconfig.xml"))
        stopwords = File.read(File.join(File.dirname(__FILE__),"..","..","..","config","stopwords.txt"))
        solrconfig_digest = Digest::SHA1.hexdigest(solrconfig)
        stopwords_digest = Digest::SHA1.hexdigest(stopwords)
        
        models_to_upload = []
        
        if column_family.to_sym == :all
          # Ensure all models are loaded
          Dir[Rails.root.join("app","models",'*.rb').to_s].each do |file| 
            require File.basename(file, File.extname(file))
          end
          
          models_to_upload += DatastaxRails::Base.models
        else
          models_to_upload << column_family.constantize
        end
        
        puts "models: #{models_to_upload.collect(&:to_s).join(",")}"
        
        models_to_upload.each do |model|
          if model.payload_model?
            next if model == DatastaxRails::PayloadModel
            unless connection.schema.column_families[model.column_family.to_s]
              puts "Creating payload model #{model.column_family}"
              columns = {:chunk => :int, :payload => :text}
              cql = DatastaxRails::Cql::CreateColumnFamily.new(model.column_family).key_name(:digest).key_columns("digest\", \"chunk").key_type(:text).columns(columns).with("COMPACT STORAGE").to_cql
              puts cql
              connection.execute_cql_query(cql)
            end
          else
            newcf = false
            newschema = false
            unless connection.schema.column_families[model.column_family.to_s]
              newcf = true
              puts "Creating normal model #{model.column_family}"
              cql = DatastaxRails::Cql::CreateColumnFamily.new(model.column_family).key_type(:text).columns(:updated_at => :text, :created_at => :text).to_cql
              puts cql
              connection.execute_cql_query(cql)
              sleep(5) if Rails.env.production?
            end
            schema = generate_solr_schema(model)
            schema_digest = Digest::SHA1.hexdigest(schema)
            
            results = DatastaxRails::Cql::Select.new(SchemaMigration, ['*']).conditions(:KEY => model.column_family).execute
            sm_digests = CassandraCQL::Result.new(results).fetch.try(:to_hash) || {}
            
            solr_url = "#{DatastaxRails::Base.solr_base_url}/resource/#{DatastaxRails::Base.config[:keyspace]}.#{model.column_family}"
            uri = URI.parse(solr_url)
            http = Net::HTTP.new(uri.host, uri.port)
            if uri.scheme == 'https'
              http.use_ssl = true
              http.cert = OpenSSL::X509::Certificate.new(Rails.root.join("config","datastax_rails.crt").read)
              http.key = OpenSSL::PKey::RSA.new(Rails.root.join("config","datastax_rails.key").read)
              http.ca_path = Rails.root.join("config","sade_ca.crt").to_s
              http.verify_mode = OpenSSL::SSL::VERIFY_NONE
            end
            http.read_timeout = 300
            if force || solrconfig_digest != sm_digests['solrconfig']
              loop do 
                puts "Posting Solr Config file to '#{solr_url}/solrconfig.xml'"
                http.post(uri.path+"/solrconfig.xml", solrconfig)
                if Rails.env.production?
                  sleep(5)
                  resp = http.get(uri.path+"/solrconfig.xml")
                  continue unless resp.message == 'OK'                  
                end
                break
              end
              DatastaxRails::Cql::Update.new(SchemaMigration, model.column_family).columns(:solrconfig => solrconfig_digest).execute
            end
            if force || stopwords_digest != sm_digests['stopwords']
              loop do
                puts "Posting Solr Stopwords file to '#{solr_url}/stopwords.txt'"
                http.post(uri.path+"/stopwords.txt", stopwords)
                if Rails.env.production?
                  sleep(5)
                  resp = http.get(uri.path+"/stopwords.txt")
                  continue unless resp.message == 'OK'
                end
                break
              end
              DatastaxRails::Cql::Update.new(SchemaMigration, model.column_family).columns(:stopwords => stopwords_digest).execute
            end
            if force || schema_digest != sm_digests['digest']
              loop do
                puts "Posting Solr Schema file to '#{solr_url}/schema.xml'"
                http.post(uri.path+"/schema.xml", schema)
                if Rails.env.production?
                  sleep(5)
                  resp = http.get(uri.path+"/schema.xml")
                  continue unless resp.message == 'OK'
                end
                break
              end
              DatastaxRails::Cql::Update.new(SchemaMigration, model.column_family).columns(:digest => schema_digest).execute
              newschema = true
            end
            
            if newcf
              # Create the SOLR Core
              url = "#{DatastaxRails::Base.solr_base_url}/admin/cores?action=CREATE&name=#{DatastaxRails::Base.config[:keyspace]}.#{model.column_family}"
              puts "Posting create command to '#{url}'"
              `curl -s -X POST '#{url}'`
              if Rails.env.production?
                sleep(5)
              end
            end
            
            # Check for unindexed columns
            model.attribute_definitions.each do |attribute, definition|
              if !connection.schema.column_families[model.column_family.to_s].columns.has_key?(attribute.to_s)# && 
                 #!definition.coder.options[:stored] && 
                 #!definition.coder.options[:indexed]
                 
                puts "Adding column '#{attribute}' to '#{model.column_family}'"
                DatastaxRails::Cql::AlterColumnFamily.new(model.column_family).add(attribute => :text).execute
              end
            end
          end
        end
      end

      private

      def connection
        DatastaxRails::Base.connection
      end

      def post_process_column_family(cf)
        comp_type = cf.comparator_type
        if comp_type && COMPARATOR_TYPES.has_key?(comp_type)
          cf.comparator_type = COMPARATOR_TYPES[comp_type]
        end

        comp_type = cf.subcomparator_type
        if comp_type && COMPARATOR_TYPES.has_key?(comp_type)
          cf.subcomparator_type = COMPARATOR_TYPES[comp_type]
        end

        col_type = cf.column_type
        if col_type && COLUMN_TYPES.has_key?(col_type)
          cf.column_type = COLUMN_TYPES[col_type]
        end

        cf
      end
    end
  end
end
