require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
    ('a'..'l').each do |letter|
      Hobby.create(:name => letter)
    end
    Hobby.commit_solr
  end
  
  describe "#find_each" do
    it "returns each record one at a time" do
      missed_hobbies = ('a'..'l').to_a
      @relation.find_each(:batch_size => 5) do |hobby|
        missed_hobbies.delete_if {|h| h == hobby.name}
      end
      missed_hobbies.should be_empty
    end
  end
  
  describe "#find_in_batches" do
    it "returns records in batches of the given size" do
      count = 12
      @relation.find_in_batches(:batch_size => 5) do |batch|
        batch.size.should <= 5
        count -= batch.size
      end
      count.should == 0
    end
  end
end
