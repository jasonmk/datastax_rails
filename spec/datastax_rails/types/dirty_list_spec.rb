require 'spec_helper'

describe DatastaxRails::Types::DirtyList do
  subject {described_class.new(double("record", :changed_attributes => {}), 'list', [], {})}
  
  before(:each) do
    subject << "Test String 1"
    subject << "Another Test String"
  end
  
  it { should eq(["Test String 1", "Another Test String"])}
  its('record.changed_attributes') { should include('list' => []) }
end
