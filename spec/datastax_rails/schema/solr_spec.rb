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
    it "uses a custom config if one is present" do
      model = mock_model("Article", :column_family => 'articles', :name => 'Article', :attribute_definitions => {})
      expect(subject.generate_solr_schema(model)).to match(/This is my custom schema/)
    end
  end
  
  describe "#upload_solr_configuration" do
    
  end
end