require 'spec_helper'

shared_examples_for 'default_consistency' do
  let(:prepared_statement) {double("PreparedStatement")}
  before { DatastaxRails::Base.connection.stub(:prepare => prepared_statement)}
  
  it "should default to QUORUM" do
    cql = DatastaxRails::Cql::Select.new(model_class, ["*"])
    prepared_statement.should_receive(:execute).with(:consistency => :quorum)
    cql.execute
  end
  
  it "should default to level specified by model class" do
    model_class.stub(:default_consistency => 'LOCAL_QUORUM')
    cql = DatastaxRails::Cql::Select.new(model_class, ["*"])
    prepared_statement.should_receive(:execute).with(:consistency => :local_quorum)
    cql.execute
  end
end
