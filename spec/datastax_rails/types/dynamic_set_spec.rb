require 'spec_helper'

describe DatastaxRails::Types::DynamicSet do
  subject {described_class.new(double("record", :changed_attributes => {}, :attributes => {}), 'set', [])}
  
  before(:each) do
    subject << "Test String 1"
    subject << "Another Test String"
    subject.add("Test String 1")
    subject << nil
  end
  
  it { is_expected.to eq(Set.new(["Test String 1", "Another Test String", nil]))}
  its('record.changed_attributes') { is_expected.to include('set' => Set.new) }
  its('record.attributes') { is_expected.to include('set' => Set.new(["Test String 1", "Another Test String", nil]))}
end
