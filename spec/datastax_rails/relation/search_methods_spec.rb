require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
  end
  
  describe "#limit" do
    it "should limit the page size" do
      "a".upto("l") do |letter|
        Hobby.create(:name => letter)
      end
      Hobby.commit_solr
      @relation.limit(7).all.size.should == 7
    end
  end
  
  describe "#page" do
    it "should get a particular page" do
      "a".upto("l") do |letter|
        Hobby.create(:name => letter)
      end
      Hobby.commit_solr
      @relation.per_page(3).page(2).order(:name).all.first.name.should == "d"
    end
  end
  
  describe "#group" do
    
  end
  
  describe "#order" do
    it "should return items in ascending order" do
      %w[fishing hiking boating jogging swimming chess].each do |word|
        Hobby.create(:name => word)
      end
      @relation.commit_solr
      @relation.order(:name).collect {|h| h.name}.should == %w[boating chess fishing hiking jogging swimming]
    end
    
    it "should return items in descending order" do
      %w[fishing hiking boating jogging swimming chess].each do |word|
        Hobby.create(:name => word)
      end
      @relation.commit_solr
      @relation.order(:name => :desc).collect {|h| h.name}.should == %w[swimming jogging hiking fishing chess boating]
    end
  end
  
  describe "#where" do
    it "should return documents where a field is nil (does not exist)" do
      Hobby.create(:name => 'Swimming')
      Hobby.create(:name => nil)
      @relation.commit_solr
      @relation.where(:name => nil).should_not be_empty
    end
    
    it "should return documents where a value is greater than the given value" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where(:complexity).greater_than(1.0).should_not be_empty
    end
    
    it "should return documents where a value is less than the given value" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where(:complexity).less_than(2.0).should_not be_empty
    end
    
    it "should allow arrays to be passed as OR queries" do
      %w[fishing hiking boating jogging swimming chess].each do |word|
        Hobby.create(:name => word)
      end
      @relation.commit_solr
      @relation.where(:name => ['boating', 'jogging', 'chess', 'skydiving']).size.should == 3
    end
    
    it "should handle negative numbers without breaking" do
      Hobby.create(:name => 'jogging', :complexity => -1.2)
      @relation.commit_solr
      @relation.where(:complexity).less_than(-1).should_not be_empty
    end
    
    it "should not tokenize where queries on spaces" do
      Hobby.create(:name => 'horseback riding')
      @relation.commit_solr
      @relation.where(:name => 'horseback').should be_empty
      @relation.where(:name => 'horseback riding').should_not be_empty
      @relation.where(:name => 'horseback ri*').should_not be_empty
    end
    
    it "should not tokenize where queries on spaces inside arrays" do
      Hobby.create(:name => 'horseback riding')
      @relation.commit_solr
      @relation.where(:name => ['horseback riding', 'some other hobby']).should_not be_empty
    end
    
    it "should search for values within a range" do
      Hobby.create(:name => 'jobbing', :complexity => 1.2)
      @relation.commit_solr
      @relation.where(:complexity => 1..2).should_not be_empty
      @relation.where(:complexity => 2..3).should be_empty
    end
  end
  
  describe "#where_not" do
    it "should return documents where a field has any value" do
      Hobby.create(:name => 'Swimming')
      @relation.commit_solr
      @relation.where_not(:name => nil).should_not be_empty
    end
    
    it "should return documents where none of the options are present" do
      Hobby.create(:name => 'Swimming')
      Hobby.create(:name => 'Biking')
      @relation.commit_solr
      @relation.where_not(:name => ['Swimming','Biking']).should be_empty
    end
  end
  
  describe "#fulltext" do
    it "should allow case-insensitive wildcard searches" do
      Hobby.create(:name => "Swimming")
      @relation.commit_solr
      @relation.fulltext("swimming").should_not be_empty
    end
  end
end