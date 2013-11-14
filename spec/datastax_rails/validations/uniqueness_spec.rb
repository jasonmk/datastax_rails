require 'spec_helper'

describe DatastaxRails::Base do
  describe "uniqueness validation" do
    it "should validate uniqueness" do
      Person.commit_solr
      Person.create!(:name => "Jason")
      Person.commit_solr
      Person.commit_solr
      person = Person.new(:name => "Jason")
      person.should_not be_valid
      person.name = "John"
      person.should be_valid
    end
    
    it "should allow an update to a model without triggering a uniqueness error" do
      Person.commit_solr
      p=Person.create!(:name => "Jason", :birthdate => Date.strptime("10/19/1985", '%m/%d/%Y'))
      Person.commit_solr
      p.birthdate = Date.strptime("10/19/1980", '%m/%d/%Y')
      p.save!
    end
    
    it "should not break when negative numbers are entered" do
      j = Job.new(:title => 'Mouseketeer', :position_number => -1)
      j.should be_valid
    end
    
    it "should not enforce uniqueness of blanks if specified" do
      Job.create!(:title => 'Engineer')
      Job.commit_solr
      j = Job.new(:title => 'Analyst')
      j.should be_valid
    end
    
    it "should enforce uniqueness of blanks if not instructed otherwise" do
      Boat.create!(:name => nil)
      Boat.commit_solr
      b=Boat.new
      b.should_not be_valid
    end
  end
end
