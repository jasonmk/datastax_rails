require 'spec_helper'

describe DatastaxRails::Schema::Solr do
  subject do
    DatastaxRails::Schema::Migrator.new('datastax_rails_test').tap{|m| m.verbose = false}
  end
  
  describe "#reindex_solr" do
    it "calls curl to post the reindex command" do
      url = "#{DatastaxRails::Base.solr_base_url}/admin/cores?action=RELOAD&name=datastax_rails_test.people&reindex=true&deleteAll=false"
      expect(subject).to receive(:`).with("curl -s -X POST '#{url}'")
      subject.reindex_solr(Person)
    end
  end
  
  describe "#create_solr_core" do
    it "calls curl to post the solr create core command" do
      url = "#{DatastaxRails::Base.solr_base_url}/admin/cores?action=CREATE&name=datastax_rails_test.people"
      expect(subject).to receive(:`).with("curl -s -X POST '#{url}'")
      subject.create_solr_core(Person)
    end
  end
  
  describe "#generate_solr_schema" do
    it "uses the default config" do
      model = mock_model("Foo", :column_family => 'foos', :name => 'Foo', :attribute_definitions => {})
      expect(subject.generate_solr_schema(model)).to match(/schema name="datastax_rails"/)
      expect(subject.generate_solr_schema(model)).not_to match(/application default schema/)
    end

    it "uses a custom config if one is present" do
      model = mock_model("Article", :column_family => 'articles', :name => 'Article', :attribute_definitions => {})
      expect(subject.generate_solr_schema(model)).to match(/This is my custom schema/)
    end

    describe "application default schema" do
      before do
        @builtin_schema = File.join(File.dirname(__FILE__),"..","..","..","config","schema.xml.erb")
        @app_default_schema = Rails.root.join('config','solr','application-schema.xml.erb')
        File.write(@app_default_schema, File.read(@builtin_schema) + %{<!-- application default schema -->})
      end

      it "should be used when present" do
        model = mock_model("Foo", :column_family => 'foos', :name => 'Foo', :attribute_definitions => {})
        expect(subject.generate_solr_schema(model)).to match(/schema name="datastax_rails"/)
        expect(subject.generate_solr_schema(model)).to match(/application default schema/)
      end

      after do
        FileUtils.remove_file(@app_default_schema)
      end
    end
  end
  
  describe "#upload_solr_configuration" do
    
  end
end
