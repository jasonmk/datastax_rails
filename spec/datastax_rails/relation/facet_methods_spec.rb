require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
  end

  describe "#field_facet" do
    
    it "should return facets on a field" do
      Hobby.create(:name => 'skiing')
      Hobby.create(:name => 'boating')
      Hobby.create(:name => 'fishing')
      Hobby.create(:name => 'skiing')
      Hobby.commit_solr
      @relation.field_facet(:name).all.facets['name'].should == ["skiing", 2, "boating", 1, "fishing", 1]
    end
    
    it "should allow options to be specified" do
      Hobby.create(:name => 'skiing')
      Hobby.create(:name => 'singing')
      Hobby.create(:name => 'reading')
      Hobby.commit_solr
      @relation.field_facet(:name, :prefix => 's').all.facets['name'].should == ["singing", 1, "skiing", 1]
    end
    
  end

  describe "#range_facet" do
    
    it "should return facets on a field" do
      Hobby.create(:complexity => 1.0)
      Hobby.create(:complexity => 5.0)
      Hobby.create(:complexity => 8.0)
      Hobby.create(:complexity => 9.0)
      Hobby.create(:complexity => 10.0)
      Hobby.commit_solr
      @relation.range_facet(:complexity, 1.0, 10.0, 2.0).all.facets['complexity'].should == {"counts"=>["1.0", 1, "3.0", 0, "5.0", 1, "7.0", 1, "9.0", 2], "gap"=>2.0, "start"=>1.0, "end"=>11.0}
    end

    it "should allow options to be specified" do
      Hobby.create(:complexity => 1.0)
      Hobby.create(:complexity => 5.0)
      Hobby.create(:complexity => 8.0)
      Hobby.create(:complexity => 9.0)
      Hobby.create(:complexity => 10.0)
      Hobby.commit_solr
      @relation.range_facet(:complexity, 1.0, 10.0, 2.0, :include => 'upper').all.facets['complexity'].should == {"counts"=>["1.0", 0, "3.0", 1, "5.0", 0, "7.0", 2, "9.0", 1], "gap"=>2.0, "start"=>1.0, "end"=>11.0}
    end
         
  end            
end