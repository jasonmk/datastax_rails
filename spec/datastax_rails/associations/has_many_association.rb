require 'spec_helper'

describe DatastaxRails::Base do
  describe "HasMany Associations" do
    describe "dependent => destroy" do
      it "should destroy all objects" do
        p = Person.create(:name => "jason")
        Car.create(:name => "Jeep", :person_id => p.id)
        p.destroy
        Car.count.should == 0
      end
    end
  end
end
