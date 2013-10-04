require 'spec_helper'

describe DatastaxRails::Cql::Select do
  before(:each) do
    @model_class = double("Model Class", :column_family => 'users', :default_consistency => DatastaxRails::Cql::Consistency::QUORUM)
  end
  
  it "should generate valid CQL" do
    cql = DatastaxRails::Cql::Select.new(@model_class, ["*"])
    cql.using(DatastaxRails::Cql::Consistency::QUORUM).conditions(:key => '12345').limit(1)
    cql.to_cql.should == "SELECT * FROM users WHERE \"key\" = '12345' LIMIT 1 "
  end
  
  it_has_behavior "default_consistency"
end
