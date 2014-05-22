require 'spec_helper'

describe DatastaxRails::Base do
  it "should run before_save" do
    p = Person.new(:name => "Jason")
    p.save
    p.nickname.should == "Jason"
  end
  
  it "should run after_save" do
    Person.commit_solr
    p = Person.new(:name => "Jason")
    p.save!
    p.instance_variable_get(:@after_save_ran).should == "yup"
  end
  
  it "should raise RecordNotFound when finding a bogus ID" do
    lambda { Person.find("xyzzy") }.should raise_exception(DatastaxRails::RecordNotFound)
  end
  
  describe "equality" do
    it "considers new objects to be unequal" do
      p1=Person.new(:name => 'John')
      p2=Person.new(:name => 'John')
      expect(p1).not_to eq(p2)
    end
    
    it "considers a new object to be unequal to a saved object" do
      p1=Person.create(:name => 'John')
      p2=Person.new(:name => 'John')
      expect(p1).not_to eq(p2)
    end
    
    it "considers two persisted objects to be equal if their primary keys are equal" do
      p1=Person.create(:name => 'John')
      p2=Person.find(p1.id)
      expect(p1).to eq(p2)
    end
    
    it "considers two persisted objects to be unequal if they have different primary keys" do
      p1=Person.create(:name => 'John')
      p2=Person.create(:name => 'James')
      expect(p1).not_to eq(p2)
    end
  end
end
