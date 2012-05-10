require 'spec_helper'

describe DatastaxRails::Base do
  describe "uniqueness validation" do
    it "should validate uniqueness" do
      Person.create(:name => "Jason")
      Sunspot.commit
      person = Person.new(:name => "Jason")
      person.should_not be_valid
      person.name = "John"
      person.should be_valid
    end
    
    it "should allow an update to a model without triggering a uniqueness error" do
      p=Person.create(:name => "Jason", :birthdate => Date.parse("10/19/1985"))
      Sunspot.commit
      p.birthdate = Date.parse("10/19/1980")
      p.save!
    end
  end
end
