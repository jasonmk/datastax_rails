require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, "hobbies")
    Hobby.commit_solr
  end
  
  describe "#find" do
    let(:h) {Hobby.create}
    let(:i) {Hobby.create}
    
    context "with a single id" do
      context "as a scalar" do
        it "finds the object and returns it as an object" do
          expect(Hobby.find(h.id)).to eq(h)
        end
        
        it "raises RecordNotFound for an invalid ID" do
          expect{Hobby.find("asdf")}.to raise_exception(DatastaxRails::RecordNotFound)
        end
        
        it "raises RecordNotFound for a nil ID" do
          expect{Hobby.find(nil)}.to raise_exception(DatastaxRails::RecordNotFound)
        end
      end
      
      context "as an array" do
        it "finds the object and returns it as a single-element array" do
          expect(Hobby.find([h.id])).to eq([h])
        end
      end
    end
    
    context "with multiple ids" do
      it "raises RecordNotFound if any portion of the records could not be found" do
        expect{Hobby.find(h.id, ::Cql::TimeUuid::Generator.new.next)}.to raise_exception(DatastaxRails::RecordNotFound)
      end
      
      context "as an array" do
        it "finds the objects and returns them as an array" do
          expect(Hobby.find([h.id, i.id])).to eq([h,i])
        end
      end
      
      context "as discrete parameters" do
        it "finds the objects and returns them as an array" do
          expect(Hobby.find(h.id, i.id)).to eq([h,i])
        end
      end
    end
  end
  
  describe "#first" do
    it "should return the first result if records are already loaded" do
      a_record = build_stubbed(:hobby)
      allow(@relation).to receive(:loaded?).and_return(true)
      @relation.instance_variable_set(:@results, [a_record, build_stubbed(:hobby)])
      expect(@relation.first).to eq(a_record)
    end
    
    it "should look up the first result if records are not already loaded" do
      a_record = build_stubbed(:hobby)
      allow(@relation).to receive(:loaded?).and_return(false)
      mock_relation = double(DatastaxRails::Relation, :to_a => [a_record])
      expect(@relation).to receive(:limit).with(1).and_return(mock_relation)
      expect(@relation.first).to eq(a_record)
    end
  end
  
  describe "#first!" do
    it "should raise RecordNotFound if no record is returned" do
      expect { @relation.first! }.to raise_exception(DatastaxRails::RecordNotFound)
    end
  end
  
  describe "#last" do
    it "should return the last result if records are already loaded" do
      a_record = build_stubbed(:hobby)
      allow(@relation).to receive(:loaded?).and_return(true)
      @relation.instance_variable_set(:@results, [build_stubbed(:hobby), a_record])
      expect(@relation.last).to eq(a_record)
    end
    
    it "should look up the last result if records are not already loaded" do
      a_record = build_stubbed(:hobby)
      allow(@relation).to receive(:loaded?).and_return(false)
      mock_relation = double(DatastaxRails::Relation, :to_a => [a_record])
      expect(@relation).to receive(:reverse_order).and_return(mock_relation)
      expect(mock_relation).to receive(:limit).with(1).and_return(mock_relation)
      expect(@relation.last).to eq(a_record)
    end
  end
  
  describe "#last!" do
    it "should raise RecordNotFound if no record is returned" do
      expect { @relation.last! }.to raise_exception(DatastaxRails::RecordNotFound)
    end
  end

  describe "#find_by" do
    it "finds a record by an attribute" do
      Boat.create(:name => 'Spooner')
      Boat.commit_solr
      expect(Boat.find_by(name: 'Spooner')).not_to be_nil
    end

    it "finds a record by an attribute with a space in it" do
      Boat.create(:name => 'Water Lily')
      Boat.commit_solr
      expect(Boat.find_by(name: 'Water Lily')).not_to be_nil
    end

    it "finds a record by an attribute with a colon in it" do
      Boat.create(:name => 'Dumb: Name')
      Boat.commit_solr
      expect(Boat.find_by(name: 'Dumb: Name')).not_to be_nil
    end
  end
end
