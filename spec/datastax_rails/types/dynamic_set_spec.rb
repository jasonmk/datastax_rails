require 'spec_helper'

describe DatastaxRails::Types::DynamicSet do
  subject {described_class.new(double("record", :changed_attributes => {}, :attributes => {}), 'set', [])}
  
  before(:each) do
    subject << "Test String 1"
    subject << "Another Test String"
    subject.add("Test String 1")
    subject << nil
  end
  
  it { should eq(Set.new(["Test String 1", "Another Test String", nil]))}
  its('record.changed_attributes') { should include('set' => Set.new) }
  its('record.attributes') { should include('set' => Set.new(["Test String 1", "Another Test String", nil]))}
end
