require 'spec_helper'

describe DatastaxRails::Base do
  it "should raise RecordNotFound when finding a bogus ID" do
    expect { Person.find("xyzzy") }.to raise_exception(DatastaxRails::RecordNotFound)
  end
  
  describe "equality" do
    it "considers new objects to be unequal" do
      p1=Person.new(:name => 'John')
      p2=Person.new(:name => 'John')
      expect(p1).not_to eq(p2)
    end
    
    it "considers a new object to be unequal to a saved object" do
      p1=Person.create!(:name => 'Mike')
      p2=Person.new(:name => 'Mike')
      expect(p1).not_to eq(p2)
    end
    
    it "considers two new objects to be unequal" do
      expect(Person.new).not_to eq(Person.new)
    end
    
    it "considers the same object equal to itself" do
      p=Person.new
      expect(p).to eq(p)
    end
    
    it "considers two persisted objects to be equal if their primary keys are equal" do
      Person.commit_solr
      p1=Person.create!(:name => 'Jim')
      p2=Person.find(p1.id)
      expect(p1).to eq(p2)
    end
    
    it "considers two persisted objects to be unequal if they have different primary keys" do
      Person.commit_solr
      p1=Person.create!(:name => 'Kirk')
      p2=Person.create!(:name => 'Tiberious')
      expect(p1).not_to eq(p2)
    end
  end
end
