require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
  end
  
  describe "#==" do
    it "should count two relations with the same parameters as equal" do
      @relation.where("name" => "jason").should == @relation.where("name" => "jason")
    end
  end
  
  describe "#any?" do
    it "should return true if there are records" do
      Hobby.create(:name => "fishing")
      Sunspot.commit
      @relation.any?.should be_true
    end
    
    it "should return false if there are no records" do
      @relation.any?.should be_false
    end
  end
  
  describe "#count" do
    it "should use the currently loaded result set to get count" do
      @relation.stub(:loaded? => true)
      @relation.instance_variable_set(:@results, mock("Recordset", :total_entries => 42))
      @relation.count.should == 42
    end
    
    it "should execute a fast search to determine the count" do
      mock_relation = mock(DatastaxRails::Relation)
      mock_relation.stub_chain(:to_a, :total_entries).and_return(37)
      @relation.should_receive(:limit).with(1).and_return(mock_relation)
      @relation.count.should == 37
    end
    
    it "should return the count regardless of limit" do
      Hobby.create(:name => "hiking")
      Hobby.create(:name => "boxing")
      Hobby.create(:name => "fishing")
      Hobby.create(:name => "running")
      Sunspot.commit
      @relation.count.should == 4
      @relation.limit(2).count.should == 4
    end
  end
  
  describe "#default_scope" do
    it "should return a relation that has no scope set" do
      Hobby.create(:name => "fishing")
      Sunspot.commit
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
      Sunspot.commit
      @relation.should be_many
    end
    
    it "should return false if there are zero or one records matching" do
      @relation.should_not be_many
      Hobby.create(:name => "hiking")
      Sunspot.commit
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
      Sunspot.commit
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
      Sunspot.commit
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
end
