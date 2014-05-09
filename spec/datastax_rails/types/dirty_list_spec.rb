require 'spec_helper'

describe DatastaxRails::Types::DirtyList do
  subject {described_class.new(double("record", :changed_attributes => {}), 'list', [])}
  
  before(:each) do
    subject << "Test String 1"
    subject << "Another Test String"
    subject << "Test String 1"
  end
  
  it { should eq(["Test String 1", "Another Test String", "Test String 1"])}
  its('record.changed_attributes') { should include('list' => []) }
  
  it "preserves ordering" do
    subject[1] = "Test String 2"
    subject.should eq(["Test String 1", "Test String 2", "Test String 1"])
  end
end
