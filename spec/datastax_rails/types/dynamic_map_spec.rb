require 'spec_helper'

describe DatastaxRails::Types::DynamicMap do
  subject {described_class.new(double("record", :changed_attributes => {}, :attributes => {}), 'map', {})}
  
  before(:each) do
    subject['mapkey'] = "Test String"
  end
  
  its(['mapkey']) { should eq("Test String")}
  its('record.changed_attributes') { should include('map' => {}) }
  its('record.attributes') { should include('map' => {'mapkey' => 'Test String'})}
  
  it "automatically maps key names when setting values" do
    subject['test'] = 'Test String'
    expect(subject).to have_key('maptest')
  end
  
  it "automatically maps key names when reading values" do
    expect(subject['key']).to eq('Test String')
  end
end
