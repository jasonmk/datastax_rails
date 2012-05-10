require 'spec_helper'

describe DatastaxRails::Base do
  describe "Cassandra finder methods" do
    before(:each) do
      @p1=Person.create(:name => "Jason")
      @p2=Person.create(:name => "John")
    end
    
    it "should find by an array of ids" do
      Person.find([@p1.id,@p2.id]).should == [@p1,@p2]
    end
    
    it "should skip ids that don't exist" do
      Person.find([@p1.id,@p2.id,"1234"]).should == [@p1,@p2]
    end
    
  end
end
