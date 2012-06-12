require 'spec_helper'

describe DatastaxRails::Base do
  describe "Relations" do
    describe "belongs_to" do
      it "should set the id when setting the object" do
        person = Person.create(:name => "Jason")
        job = Job.create(:title => "Developer")
        job.person = person
        job.person_id.should == person.id
      end
      
      it "should look up the owning model by id" do
        person = Person.create(:name => "John")
        job = Job.create(:title => "Developer", :person_id => person.id)
        Person.scoped.commit_solr
        Job.first.person.should == person
      end
    end
  end
end
