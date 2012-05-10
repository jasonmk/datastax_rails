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
          if coder.options[:solr_type] && (coder.options[:indexed] || coder.options[:stored])
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
            @copy_fields.push({ :source => attr.name, :dest => "sort_" + attr.name })
          end
          if coder.options[:fulltext]
            @fulltext_fields << attr.name
          end
        end
        return ERB.new(File.read(File.join(File.dirname(__FILE__),"..","..","..","config","schema.xml.erb"))).result(binding)
      end
      
      def upload_solr_schemas(column_family = :all)
        solrconfig = File.read(File.join(File.dirname(__FILE__),"..","..","..","config","solrconfig.xml"))
        stopwords = File.read(File.join(File.dirname(__FILE__),"..","..","..","config","stopwords.txt"))
        models_to_upload = []
        
        if column_family == :all
          # Ensure all models are loaded
          Dir[Rails.root.join("app","models",'*.rb').to_s].each do |file| 
            require File.basename(file, File.extname(file))
          end
          
          models_to_upload += DatastaxRails::Base.models
        else
          models_to_upload << column_family.constantize
        end
        
        models_to_upload.each do |model|
          schema = generate_solr_schema(model)
          solr_url = "#{DatastaxRails::Base.config[:solr][:url]}/resource/#{SolandraObject::Base.config[:keyspace]}.#{model.column_family}"
          uri = URI.parse(solr_url)
          Net::HTTP.start(uri.host, uri.port) do |http|
            puts "Posting Solr Config file to '#{uri.path}/solrconfig.xml'"
            http.post(uri.path+"/solrconfig.xml", solrconfig)
            puts "Posting Solr Stopwords file to '#{uri.path}/stopwords.txt'"
            http.post(uri.path+"/stopwords.txt", stopwords)
            puts "Posting Solr Schema file to '#{uri.path}/schema.xml'"
            http.post(uri.path+"/schema.xml", schema)
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
