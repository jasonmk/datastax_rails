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
      expect(Car.count).to eq(0)
    end
    
    it "should retrieve child records" do
      p = Person.create(:name => 'jason')
      c = Car.create(:name => 'Jeep', :person_id => p.id)
      Car.commit_solr
      expect(p.cars).to include(c)
    end
    
    it "should retrieve only child records" do
      p = Person.create(:name => 'jason')
      c = Car.create(:name => 'Jeep', :person_id => '12345')
      Car.commit_solr
      expect(p.cars).not_to include(c)
    end
    
    it "should create records with the proper foreign key" do
      Person.commit_solr
      p = Person.create(:name => 'jason')
      p.cars.create(:name => 'Jeep')
      Car.commit_solr
      Person.commit_solr
      expect(Car.first.person).to eq(p)
    end
  end
end
