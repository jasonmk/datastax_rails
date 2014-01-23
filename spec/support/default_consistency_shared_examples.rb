require 'spec_helper'

shared_examples_for 'default_consistency' do
  it "should default to QUORUM" do
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(an_instance_of(String), :consistency => :quorum)
    cql.execute
  end
  
  it "should default to level specified by model class" do
    @model_class.stub(:default_consistency => 'LOCAL_QUORUM')
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(an_instance_of(String), :consistency => :local_quorum)
    cql.execute
  end
end
