require 'spec_helper'

describe DatastaxRails::Types::FloatType do
  before(:each) do
    @coder = DatastaxRails::Types::FloatType.new
  end
  
  describe "#encode" do
    it "should store decimals as strings" do
      @coder.encode(12.0).should eq("12.0")
    end
    
    it "should raise an exception on improperly formatted strings" do
      lambda { @coder.encode("foo") }.should raise_exception(ArgumentError)
    end
    
    it "should store a sentinel value for nils" do
      @coder.encode(nil).should eq('-10191980.0')
    end
  end
  
  describe "#decode" do
    it "should return floats" do
      @coder.decode("12.0").should be_within(0.1).of(12.0)
    end
    
    it "should return nil if the sentinel value is found" do
      @coder.decode("-10191980.0").should be_nil
    end
  end
end
