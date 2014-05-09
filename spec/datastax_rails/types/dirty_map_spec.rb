require 'spec_helper'

describe DatastaxRails::Types::DirtyMap do
  subject {described_class.new(double("record", :changed_attributes => {}, :attributes => {}), 'map', {})}
  
  before(:each) do
    subject['key'] = "Test String"
  end
  
  its(['key']) { should eq("Test String")}
  its('record.changed_attributes') { should include('map' => {}) }
  its('record.attributes') { should include('map' => {'key' => 'Test String'})}
end
