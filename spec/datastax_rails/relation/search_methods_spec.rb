require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
  end
  
  describe "#consistency" do
    it "should throw an ArgumentError for invalid consistency levels" do
      lambda { @relation.consistency(:foo) }.should raise_exception(ArgumentError)
    end
    
    it "should not raise an exception for a valid consistency level" do
      lambda { @relation.consistency(:local_quorum) }.should_not raise_exception
    end
    
    it "should call cassandra to enforce consistency" do
      h=Hobby.create(:name => 'swimming')
      Hobby.commit_solr
      Hobby.stub_chain(:with_cassandra,:consistency).and_return(@relation)
      @relation.should_receive(:find_by_id).with(h.id)
      @relation.consistency(:all).where(:name => 'swimming').all
    end
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
    
    it "should allow :greater_than to be specified in a single call" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where(:complexity => {:greater_than => 1.0}).should_not be_empty
    end
    
    it "should return documents where a value is less than the given value" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where(:complexity).less_than(2.0).should_not be_empty
    end
    
    it "should allow :less_than to be specified in a single call" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where(:complexity => {:less_than => 2.0}).should_not be_empty
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
      Hobby.create(:name => 'jogging', :complexity => 1.2)
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
    
    it "should return documents where a value is not greater than the given value" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where_not(:complexity).greater_than(2.0).should_not be_empty
    end
    
    it "should allow :greater_than to be specified in a single call" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where_not(:complexity => {:greater_than => 2.0}).should_not be_empty
    end
    
    it "should return documents where a value is not less than the given value" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where_not(:complexity).less_than(1.0).should_not be_empty
    end
    
    it "should allow :less_than to be specified in a single call" do
      Hobby.create(:name => 'Swimming', :complexity => 1.1)
      @relation.commit_solr
      @relation.where_not(:complexity => {:less_than => 1.0}).should_not be_empty
    end
    
    it "should search for values outside a range" do
      Hobby.create(:name => 'jogging', :complexity => 1.2)
      @relation.commit_solr
      @relation.where_not(:complexity => 1..2).should be_empty
      @relation.where_not(:complexity => 2..3).should_not be_empty
    end
  end
  
  describe "#fulltext" do
    it "should allow case-insensitive wildcard searches" do
      Hobby.create(:name => "Swimming")
      @relation.commit_solr
      @relation.fulltext("swimming").should_not be_empty
    end
  end
  
  describe '#highlight' do
    let(:hl) { @relation.highlight(:name, :description, :snippet => 3, :fragsize => 200) }
    
    it { expect(hl.highlight_options[:fields]).to eq [:name, :description] }
    it { expect(hl.highlight_options[:snippet]).to eq 3 }
    it { expect(hl.highlight_options[:fragsize]).to eq 200 }
    
    context 'with duplicate fields' do
      let(:hl) { @relation.highlight(:name, :description, :name) }
      
      it { expect(hl.highlight_options[:fields]).to eq [:name, :description] }
    end
  end
  
  describe '#solr_format' do
    context 'when formatting Time' do
      let(:time) { Time.new 2011, 10, 9, 8, 7, 6, "-05:00" }
      
      it { expect(@relation.solr_format(time)).to eq '2011-10-09T13:07:06Z' }
    end
    
    context 'when formatting Date' do
      let(:date) { Date.new 2001, 2, 3 }
      
      it { expect(@relation.solr_format(date)).to eq '2001-02-03T00:00:00Z' }
    end
    
    context 'when formatting DateTime' do
      let(:datetime) { DateTime.new 2001, 2, 3, 4, 5, 6, "-07:00" }
      
      it { expect(@relation.solr_format(datetime)).to eq '2001-02-03T11:05:06Z' }
    end
  end
end