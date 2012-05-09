require 'spec_helper'

describe DatastaxRails::Base do
  describe "Cassandra finder methods" do
    before(:each) do
      @p1=Person.create(:name => "Jason")
      @p2=Person.create(:name => "John")
      Sunspot.commit
    end
    
    it "should find by an array of ids" do
      Person.multi_find([@p1.id,@p2.id]).should == [@p1,@p2]
    end
    
    it "should skip ids that don't exist" do
      Person.multi_find([@p1.id,@p2.id,"1234"]).should == [@p1,@p2]
    end
    
    it "should remove invalid records from Sunspot" do
      Sunspot.should_receive(:remove_by_id).with("Person","1234")
      Person.multi_find([@p1.id,@p2.id,"1234"])
    end
  end
end
