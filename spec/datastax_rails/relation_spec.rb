require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
    @relation.commit_solr
  end
  
  describe "#==" do
    it "should count two relations with the same parameters as equal" do
      @relation.where("name" => "jason").should == @relation.where("name" => "jason")
    end
  end
  
  describe "#any?" do
    it "should return true if there are records" do
      Hobby.create(:name => "fishing")
      @relation.commit_solr
      @relation.any?.should be_true
    end
    
    it "should return false if there are no records" do
      @relation.any?.should be_false
    end
  end
  
  describe "#count" do
    it "should use the cached count if it is available" do
      @relation.instance_variable_set(:@count, 42)
      @relation.count.should == 42
    end
    
    it "should cache the total count on any solr query" do
      @relation.should_receive(:query_via_solr).and_return(double("ResultSet", :total_entries => 42))
      @relation.all
      @relation.count.should == 42
    end
    
    it "should execute a fast search to determine the count" do
      mock_relation = double(DatastaxRails::Relation)
      mock_relation.stub_chain(:select, :to_a, :total_entries).and_return(37)
      @relation.should_receive(:limit).with(1).and_return(mock_relation)
      @relation.count.should == 37
    end
    
    it "should return the count regardless of limit" do
      Hobby.create(:name => "hiking")
      Hobby.create(:name => "boxing")
      Hobby.create(:name => "fishing")
      Hobby.create(:name => "running")
      @relation.commit_solr
      @relation.count.should == 4
      
      @relation.limit(2).count.should == 4
    end
  end
  
  describe "#default_scope" do
    it "should return a relation that has no scope set" do
      Hobby.create(:name => "fishing")
      @relation.commit_solr
      relation = @relation.where("name" => "hiking")
      relation.count.should == 0
      relation.default_scope.count.should == 1
    end
  end
  
  describe "#empty?" do
    it "should use the loaded result set to determine emptiness" do
      a_record = mock_model(Hobby)
      @relation.stub(:loaded? => true)
      @relation.instance_variable_set(:@results, [])
      @relation.should be_empty
      @relation.instance_variable_set(:@results, [a_record])
      @relation.should_not be_empty
    end
  end
  
  describe "#many?" do
    it "should return true if there are multiple records matching" do
      Hobby.create(:name => "hiking")
      Hobby.create(:name => "swimming")
      @relation.commit_solr
      @relation.should be_many
    end
    
    it "should return false if there are zero or one records matching" do
      @relation.should_not be_many
      Hobby.create(:name => "hiking")
      @relation.should_not be_many
    end
  end
  
  describe "#new" do
    it "should instantiate a new instance of the class" do
      hiking = @relation.new(:name => "hiking")
      hiking.should be_a_kind_of(Hobby)
      hiking.name.should == "hiking"
    end
  end
  
  describe "#reload" do
    it "should reload the results" do
      @relation.all.should be_empty
      Hobby.create(:name => "hiking")
      @relation.commit_solr
      @relation.all.should be_empty
      @relation.reload.all.should_not be_empty
    end
  end
  
  describe "#size" do
    it "should return the size of the current result set (including limit setting)" do
      Hobby.create(:name => "hiking")
      Hobby.create(:name => "boxing")
      Hobby.create(:name => "fishing")
      Hobby.create(:name => "running")
      @relation.commit_solr
      @relation.size.should == 4
      @relation.limit(2).size.should == 2
    end
  end
  
  describe "#total_pages" do
    it "should calculate the total number of pages for will_paginate" do
      relation = @relation.per_page(30)
      relation.stub(:count => 100)
      relation.total_pages.should == 4
    end
  end
  
  describe "grouped queries" do
    before(:each) do
      Person.commit_solr
      Person.create(:name => 'John', :nickname => 'J')
      Person.create(:name => 'Jason', :nickname => 'J')
      Person.create(:name => 'James', :nickname => 'J')
      Person.create(:name => 'Kathrine', :nickname => 'Kat')
      Person.create(:name => 'Kathy', :nickname => 'Kat')
      Person.create(:name => 'Steven', :nickname => 'Steve')
      Person.commit_solr
    end
    
    it "should return matching documents grouped by an attribute" do
      results = Person.group(:nickname).all
      results['j'].should have(3).items
      results['kat'].should have(2).items
      results['steve'].should have(1).item
    end
    
    it "should return total_entries as the highest value of any group" do
      results = Person.group(:nickname).all
      results.total_entries.should eq(3)
    end
    
    it "should still return a total count when using the count method" do
      results = Person.group(:nickname).count.should eq(6)
    end
  end
end
