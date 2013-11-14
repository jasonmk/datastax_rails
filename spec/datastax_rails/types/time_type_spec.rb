require 'spec_helper'

describe DatastaxRails::Types::TimeType do
  let(:coder) { DatastaxRails::Types::TimeType.new }
  let(:utc_time) { Time.utc(2013, 12, 11, 10, 9, 8) }
  
  describe "#encode" do
    let(:local_time) { Time.new(2011, 10, 9, 8, 7, 6, "-05:00") }
    
    it { expect(coder.encode(nil)).to be_nil }
    it { expect{coder.encode("bad time")}.to raise_error(ArgumentError) }
    it { expect(coder.encode(utc_time)).to eq "2013-12-11T10:09:08Z" }
    it { expect(coder.encode(local_time)).to eq "2011-10-09T13:07:06Z" }
  end
  
  describe "#decode" do
    let(:time) { "2013-12-11T10:09:08Z" }
    
    it { expect(coder.decode(time)).to eq utc_time }
    it { expect(coder.decode(utc_time)).to eq utc_time }
    
    context "when timezone is not UTC" do
      before(:each) { Time.zone = "Eastern Time (US & Canada)" }
      
      it { expect(coder.decode(time)).to eq Time.new(2013, 12, 11, 5, 9, 8, '-05:00') }
    end
  end
end
