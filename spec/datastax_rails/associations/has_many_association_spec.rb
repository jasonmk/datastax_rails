require 'spec_helper'

describe DatastaxRails::Base do
  describe "HasMany Associations" do
    it "should destroy all objects with :dependent => :destroy" do
      p = Person.create(:name => "jason")
      Car.create(:name => "Jeep", :person_id => p.id)
      Car.commit_solr
      p.destroy
      Car.commit_solr
      Person.commit_solr
      Car.count.should == 0
    end
    
    it "should retrieve child records" do
      p = Person.create(:name => 'jason')
      c = Car.create(:name => 'Jeep', :person_id => p.id)
      Car.commit_solr
      p.cars.should include(c)
    end
    
    it "should retrieve only child records" do
      p = Person.create(:name => 'jason')
      c = Car.create(:name => 'Jeep', :person_id => '12345')
      Car.commit_solr
      p.cars.should_not include(c)
    end
    
    it "should create records with the proper foreign key" do
      Person.commit_solr
      p = Person.create(:name => 'jason')
      p.cars.create(:name => 'Jeep')
      Car.commit_solr
      Person.commit_solr
      Car.first.person.should == p
    end
  end
end
