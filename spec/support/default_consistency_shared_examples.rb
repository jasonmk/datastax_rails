require 'spec_helper'

shared_examples_for 'default_consistency' do
  let(:prepared_statement) {double("PreparedStatement")}
  before { allow(DatastaxRails::Base.connection).to receive(:prepare).and_return(prepared_statement)}
  
  it "should default to QUORUM" do
    cql = DatastaxRails::Cql::Select.new(model_class, ["*"])
    expect(prepared_statement).to receive(:execute).with(:consistency => :quorum)
    cql.execute
  end
  
  it "should default to level specified by model class" do
    allow(model_class).to receive(:default_consistency).and_return('LOCAL_QUORUM')
    cql = DatastaxRails::Cql::Select.new(model_class, ["*"])
    expect(prepared_statement).to receive(:execute).with(:consistency => :local_quorum)
    cql.execute
  end
end
