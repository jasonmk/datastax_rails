require 'spec_helper'

shared_examples_for 'default_consistency' do
  it "should default to QUORUM" do
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    DatastaxRails::Base.connection.should_receive(:prepare).with(anything, :consistency => :quorum)
    cql.execute
  end
  
  it "should default to level specified by model class" do
    @model_class.stub(:default_consistency => 'LOCAL_QUORUM')
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    DatastaxRails::Base.connection.should_receive(:prepare).with(anything, :consistency => :local_quorum)
    cql.execute
  end
end
