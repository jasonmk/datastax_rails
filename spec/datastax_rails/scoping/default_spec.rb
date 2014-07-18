require 'spec_helper.rb'

describe DatastaxRails::Base do
  context "Scoping" do
    context "Default" do
      it "applies the default scope" do
        Boat.create(:name => 'WindDancer', :registration => 1)
        Boat.create(:name => 'Misty', :registration => 2)
        Boat.create(:name => 'Voyager', :registration => 3)
        Boat.create(:name => 'Aquacadabra', :registration => 4)
        Boat.commit_solr
        
        expect(Boat.where(:registration => [1,2,3,4]).collect(&:name)).to eq(['Aquacadabra', 'Misty', 'Voyager', 'WindDancer'])
      end
    end
  end
end
