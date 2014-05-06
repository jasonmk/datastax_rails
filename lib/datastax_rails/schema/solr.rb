module DatastaxRails
  module Schema
    module Solr
      # Generates a SOLR schema file.  The default schema template included with DSR can handle
      # most normal circumstances for indexing.  When a customized template is required, it can
      # be placed in the application's config/solr directory using the naming convention
      # column_family-schema.xml.erb.  It will be processed as a normal ERB file.  See the DSR version
      # for examples.  
      def generate_solr_schema(model)
        @fields = []
        @copy_fields = []
        @fulltext_fields = []
        @primary_key = model.primary_key.gsub(/ *(asc|desc)/i, '')
        @custom_fields = ""
        @columns = model.attribute_definitions.values
        # model.attribute_definitions.values.each do |column|
          # if column.options[:solr_index] || column.options[:solr_store]
            # @fields.push({ :name => column.name,
                           # :type => column.solr_type,
                           # :indexed => column.options[:solr_index].to_s,
                           # :stored => column.options[:solr_store].to_s,
                           # :multi_valued => column.options[:multi_valued].to_s })
          # end
          # if column.options[:sortable] && column.options[:tokenized]
            # @fields.push({ :name => "sort_" + column.name,
                           # :type => "string",
                           # :indexed => true,
                           # :stored => false,
                           # :multi_valued => false })
            # @copy_fields.push({ :source => column.name, :dest => "sort_" + column.name }) if (column.options[:indexed] || column.options[:stored])
          # end
          # if column.options[:fulltext]
            # @fulltext_fields << column.name if (column.options[:indexed])
          # end
        # end
        # Sort the fields so that no matter what order the attributes are arranged into the
        # same schema file gets generated
        @fields.sort! {|a,b| a[:name] <=> b[:name]}
        @copy_fields.sort! {|a,b| a[:source] <=> b[:source]}
        @fulltext_fields.sort!
        
        if Rails.root.join('config','solr',"#{model.column_family}-schema.xml.erb").exist?
          say "Using custom schema for #{model.name}", :subitem
          ERB.new(Rails.root.join('config','solr',"#{model.column_family}-schema.xml.erb").read, 0, '>').result(binding)
        else
          ERB.new(File.read(File.join(File.dirname(__FILE__),"..","..","..","config","schema.xml.erb")), 0, '>').result(binding)
        end
      end
      
      # Sends a command to Solr instructing it to reindex the data.  The data is reindexed in the background,
      # and the new index is swapped in once it is finished.
      def reindex_solr(model, destructive=false)
        url = "#{DatastaxRails::Base.solr_base_url}/admin/cores?action=RELOAD&name=#{DatastaxRails::Base.config[:keyspace]}.#{model.column_family}&reindex=true&deleteAll=#{destructive.to_s}"
        say "Posting reindex command to '#{url}'", :subitem
        `curl -s -X POST '#{url}'`
        say "Reindexing will run in the background", :subitem
      end
      
      # Creates the initial Solr Core.  This is required once the first time a Solr schema is uploaded.
      # It will cause the data to be indexed in the background.
      def create_solr_core(model)
        url = "#{DatastaxRails::Base.solr_base_url}/admin/cores?action=CREATE&name=#{DatastaxRails::Base.config[:keyspace]}.#{model.column_family}"
        say "Posting create command to '#{url}'", :subitem
        `curl -s -X POST '#{url}'`
      end
      
      # Uploads the necessary configuration files for solr to function
      # The solrconfig and stopwords files can be overridden on a per-model basis
      # by creating a file called config/solr/column_family-solrconfig.xml or
      # config/solr/column_family-stopwords.txt
      #
      # TODO: find a way to upload arbitrary files automatically (e.g., additional stopwords lists)
      def upload_solr_configuration(model, force=false)
        count = 0
        if Rails.root.join('config','solr',"#{model.column_family}-solrconfig.xml").exist?
          say "Using custom solrconfig file", :subitem
          solrconfig = Rails.root.join('config','solr',"#{model.column_family}-solrconfig.xml").read
        else
          @legacy = model.legacy_mapping?
          solrconfig = ERB.new(File.read(File.join(File.dirname(__FILE__),"..","..","..","config","solrconfig.xml.erb"))).result(binding)
        end
        if Rails.root.join('config','solr',"#{model.column_family}-stopwords.txt").exist?
          say "Using custom stopwords file", :subitem
          stopwords = Rails.root.join('config','solr',"#{model.column_family}-stopwords.txt").read
        else
          stopwords = File.read(File.join(File.dirname(__FILE__),"..","..","..","config","stopwords.txt"))
        end
        schema = generate_solr_schema(model)
        solrconfig_digest = Digest::SHA1.hexdigest(solrconfig)
        stopwords_digest = Digest::SHA1.hexdigest(stopwords)
        schema_digest = Digest::SHA1.hexdigest(schema)
        
        newcf = !column_exists?(model.column_family, 'solr_query')
        force ||= newcf
        
        results = DatastaxRails::Cql::Select.new(SchemaMigration, ['*']).conditions(:cf => model.column_family).execute
        sm_digests = results.first || {}
        
        solr_url = "#{DatastaxRails::Base.solr_base_url}/resource/#{@keyspace}.#{model.column_family}"
        
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
          count += 1
          loop do 
            say "Posting Solr Config file to '#{solr_url}/solrconfig.xml'", :subitem
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
          count += 1
          loop do
            say "Posting Solr Stopwords file to '#{solr_url}/stopwords.txt'", :subitem
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
          count += 1
          loop do
            say "Posting Solr Schema file to '#{solr_url}/schema.xml'", :subitem
            http.post(uri.path+"/schema.xml", schema)
            if Rails.env.production?
              sleep(5)
              resp = http.get(uri.path+"/schema.xml")
              continue unless resp.message == 'OK'
            end
            break
          end
          DatastaxRails::Cql::Update.new(SchemaMigration, model.column_family).columns(:digest => schema_digest).execute
          newcf ? create_solr_core(model) : reindex_solr(model)
        end
        count
      end
    end
  end
end