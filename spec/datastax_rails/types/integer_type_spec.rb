require 'spec_helper'

describe DatastaxRails::Types::IntegerType do
  before(:each) do
    @coder = DatastaxRails::Types::IntegerType.new
  end
  
  describe "#encode" do
    it "should store integers as strings" do
      @coder.encode(12).should eq("12")
    end
    
    it "should raise an exception on improperly formatted strings" do
      lambda { @coder.encode("foo") }.should raise_exception(ArgumentError)
    end
    
    it "should store a sentinel value for nils" do
      @coder.encode(nil).should  eq("-10191980")
    end
  end
  
  describe "#decode" do
    it "should return integers" do
      @coder.decode("12").should eq(12)
    end
    
    it "should return nil if the sentinel value is found" do
      @coder.decode("-10191980").should be_nil
    end
  end
end
