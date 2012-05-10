require 'spec_helper'

describe DatastaxRails::Cql::Select do
  before(:each) do
    @model_class = mock("Model Class", :column_family => 'users')
  end
  it "should generate valid CQL" do
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    cql.using(DatastaxRails::Cql::Consistency::QUORUM).conditions(:key => '12345').limit(1)
    cql.to_cql.should == "SELECT * FROM users USING CONSISTENCY QUORUM WHERE key = '12345' LIMIT 1"
  end
end
