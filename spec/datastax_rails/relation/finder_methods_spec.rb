require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
    Hobby.commit_solr
  end
  
  describe "#first" do
    it "should return the first result if records are already loaded" do
      a_record = mock_model(Hobby)
      @relation.stub(:loaded? => true)
      @relation.instance_variable_set(:@results, [a_record, mock_model(Hobby)])
      @relation.first.should == a_record
    end
    
    it "should look up the first result if records are not already loaded" do
      a_record = mock_model(Hobby)
      @relation.stub(:loaded? => false)
      mock_relation = double(DatastaxRails::Relation, :to_a => [a_record])
      @relation.should_receive(:limit).with(1).and_return(mock_relation)
      @relation.first.should == a_record
    end
  end
  
  describe "#first!" do
    it "should raise RecordNotFound if no record is returned" do
      lambda { @relation.first! }.should raise_exception(DatastaxRails::RecordNotFound)
    end
  end
  
  describe "#last" do
    it "should return the last result if records are already loaded" do
      a_record = mock_model(Hobby)
      @relation.stub(:loaded? => true)
      @relation.instance_variable_set(:@results, [mock_model(Hobby), a_record])
      @relation.last.should == a_record
    end
    
    it "should look up the last result if records are not already loaded" do
      a_record = mock_model(Hobby)
      @relation.stub(:loaded? => false)
      mock_relation = double(DatastaxRails::Relation, :to_a => [a_record])
      @relation.should_receive(:reverse_order).and_return(mock_relation)
      mock_relation.should_receive(:limit).with(1).and_return(mock_relation)
      @relation.last.should == a_record
    end
  end
  
  describe "#last!" do
    it "should raise RecordNotFound if no record is returned" do
      lambda { @relation.last! }.should raise_exception(DatastaxRails::RecordNotFound)
    end
  end

  describe "#find_by_" do
    it "finds a record by an attribute" do
      Boat.create(:name => 'Spooner')
      Boat.commit_solr
      Boat.find_by_name('Spooner').should_not be_nil
    end

    it "finds a record by an attribute with a space in it" do
      Boat.create(:name => 'Water Lily')
      Boat.commit_solr
      Boat.find_by_name('Water Lily').should_not be_nil
    end

    it "finds a record by an attribute with a colon in it" do
      Boat.create(:name => 'Dumb: Name')
      Boat.commit_solr
      Boat.find_by_name('Dumb: Name').should_not be_nil
    end
  end
end
