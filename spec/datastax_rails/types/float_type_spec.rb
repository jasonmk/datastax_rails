require 'spec_helper'

describe DatastaxRails::Types::FloatType do
  before(:each) do
    @coder = DatastaxRails::Types::FloatType.new
  end
  
  describe "#encode" do
    it "should store decimals as Floats" do
      @coder.encode(12.0).should be_a_kind_of(Float)
    end
    
    it "should convert properly formatted strings to Floats" do
      @coder.encode("12.0").should be_a_kind_of(Float)
    end
    
    it "should raise an exception on improperly formatted strings" do
      lambda { @coder.encode("foo") }.should raise_exception(ArgumentError)
    end
    
    it "should store a sentinel value for nils" do
      @coder.encode(nil).should be_within(0.1).of(-10191980.0)
    end
  end
  
  describe "#decode" do
    it "should return floats" do
      @coder.decode(12.0).should be_within(0.1).of(12.0)
    end
    
    it "should return nil if the sentinel value is found" do
      @coder.decode(-10191980.0).should be_nil
    end
  end
end
