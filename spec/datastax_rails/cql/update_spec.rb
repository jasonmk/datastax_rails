require 'spec_helper'

describe DatastaxRails::Cql::Update do
  before(:each) do
    @model_class = mock("Model Class", :column_family => 'users', :default_consistency => DatastaxRails::Cql::Consistency::QUORUM)
  end
  
  it "should generate valid CQL" do
    cql = DatastaxRails::Cql::Update.new(@model_class, "12345")
    cql.using(DatastaxRails::Cql::Consistency::QUORUM).columns(:name => 'John', :age => '23')
    cql.to_cql.should == "update users using consistency QUORUM SET name = 'John', age = '23' WHERE KEY IN ('12345')"
  end
  
  it_has_behavior "default_consistency"
end
