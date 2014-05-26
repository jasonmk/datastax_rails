require 'spec_helper'

class DynamicTestModel1 < DatastaxRails::DynamicModel
  self.grouping = 'test1'
  string :name
  timestamps
end

class DynamicTestModel2 < DatastaxRails::DynamicModel
  self.grouping = 'test2'
end

describe DatastaxRails::DynamicModel do
  let(:one) {DynamicTestModel1.new}
  let(:two) {DynamicTestModel2.new}
  
  it {expect(one).to respond_to(:created_at)}
  it {expect(one).to respond_to(:created_at=)}
  it {expect(two).not_to respond_to(:created_at)}
  it {expect(two).not_to respond_to(:created_at=)}
  
  it "sets the attribute in the dynamic collection" do
    one.name = 'John'
    expect(one.s_).to eq('s_name' => 'John')
  end
  
  it "retrieves the attribute from the dynamic collection" do
    one.strings[:name] = 'John'
    expect(one.name).to eq('John')
  end
  
  describe "#solr_field_name" do
    it "maps a attribute name to the underlying storage key" do
      expect(one.solr_field_name(:name)).to eq('s_name')
    end
    
    it "raises DatastaxRails::UnknownAttributeError if an unknown attribute is mapped without a type" do
      expect{two.solr_field_name(:name)}.to raise_exception(DatastaxRails::UnknownAttributeError)
    end
    
    it "maps an undeclared attribute if a type is given" do
      expect(one.solr_field_name(:birthdate, :date)).to eq('d_birthdate')
    end
  end
end
