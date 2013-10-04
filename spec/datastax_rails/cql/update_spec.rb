require 'spec_helper'

describe DatastaxRails::Cql::Update do
  before(:each) do
    @model_class = double("Model Class", :column_family => 'users', :default_consistency => DatastaxRails::Cql::Consistency::QUORUM)
  end
  
  it "should generate valid CQL" do
    cql = DatastaxRails::Cql::Update.new(@model_class, "12345")
    cql.using(DatastaxRails::Cql::Consistency::QUORUM).columns(:name => 'John', :age => '23')
    cql.to_cql.should match(/update users SET ("name" = 'John', "age" = '23'|"age" = '23', "name" = 'John') WHERE key IN \('12345'\)/)
  end
  
  it_has_behavior "default_consistency"
end
