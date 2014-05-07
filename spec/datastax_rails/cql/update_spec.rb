require 'spec_helper'

describe DatastaxRails::Cql::Update do
  before(:each) do
    @model_class = double("Model Class", :column_family => 'users', :default_consistency => DatastaxRails::Cql::Consistency::QUORUM, :primary_key => 'id')
  end
  
  it "should generate valid CQL" do
    cql = DatastaxRails::Cql::Update.new(@model_class, "12345")
    cql.using(DatastaxRails::Cql::Consistency::QUORUM).columns(:name => 'John', :age => '23')
    cql.to_cql.should match(/update users SET ("name" = \?, "age" = \?|"age" = \?, "name" = \?) WHERE id IN \(\?\)/)
  end
  
  it_has_behavior "default_consistency"
end
