require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
  end
  
  describe "Modification Methods" do
    describe "#destroy_all" do
      it "should destroy all matching records" do
        Hobby.create(:name => "biking", :complexity => 1.0)
        Hobby.create(:name => "skydiving", :complexity => 4.0)
        @relation.where(:complexity).greater_than(2.0).destroy_all
        @relation.commit_solr
        @relation.count.should == 1
      end
    end
    
    describe "#destroy" do
      before(:each) do
        @h1 = Hobby.create(:name => "biking", :complexity => 1.0)
        @h2 = Hobby.create(:name => "skydiving", :complexity => 4.0)
        @relation.commit_solr
      end
      
      it "should destroy 1 record by id" do
        @relation.destroy(@h1.id)
        @relation.commit_solr
        @relation.count.should == 1
      end
      
      it "should destroy multiple records by id" do
        @relation.destroy([@h1.id, @h2.id])
        @relation.commit_solr
        @relation.count.should == 0
      end
    end
  end
end
