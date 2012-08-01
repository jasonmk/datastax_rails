require 'spec_helper'

shared_examples_for 'default_consistency' do
  it "should default to QUORUM" do
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    cql.to_cql.should match(/using consistency quorum/i)
  end
  
  it "should default to level specified by model class" do
    @model_class.stub(:default_consistency => 'LOCAL_QUORUM')
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    cql.to_cql.should match(/using consistency local_quorum/i)
  end
end
