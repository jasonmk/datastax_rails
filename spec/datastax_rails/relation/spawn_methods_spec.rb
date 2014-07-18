require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
  end
  
  describe "#merge" do
    it "should merge two relations" do
      r1 = @relation.where("name" => "biking")
      r2 = @relation.order("name" => :desc)
      expect(r1.merge(r2)).to eq(@relation.where("name" => "biking").order("name" => :desc))
    end
    
    it "should merge where conditions into a single hash" do
      r1 = @relation.where("name" => "biking")
      r2 = @relation.where("complexity" => 1.0)
      expect(r1.merge(r2).where_values).to eq([{"name" => "biking", "complexity" => 1.0}])
    end
    
    it "should overwrite conditions on the same attribute" do
      r1 = @relation.where("name" => "biking")
      r2 = @relation.where("name" => "swimming")
      expect(r1.merge(r2).where_values).to eq([{"name" => "swimming"}])
      expect(r2.merge(r1).where_values).to eq([{"name" => "biking"}])
    end
  end
end
