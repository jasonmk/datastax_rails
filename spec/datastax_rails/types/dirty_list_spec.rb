require 'spec_helper'

describe DatastaxRails::Types::DirtyList do
  subject {described_class.new(double("record", :changed_attributes => {}, :attributes => {}), 'list', [])}
  
  before(:each) do
    subject << "Test String 1"
    subject << "Another Test String"
    subject << "Test String 1"
  end
  
  it { should eq(["Test String 1", "Another Test String", "Test String 1"])}
  its('record.changed_attributes') { should include('list' => []) }
  its('record.attributes') { should include('list' => ["Test String 1", "Another Test String", "Test String 1"])}
  
  it "preserves ordering" do
    subject[1] = "Test String 2"
    subject.should eq(["Test String 1", "Test String 2", "Test String 1"])
  end
end
