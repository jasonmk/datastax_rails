require 'spec_helper'

describe DatastaxRails::Column do
  describe "type casting" do
    describe "boolean" do
      let(:column) {DatastaxRails::Column.new("field", nil, "boolean")}

      it "casts '' to nil" do 
        expect(column.type_cast('')).to be_nil
      end
      
      it "casts nil to nil" do
        expect(column.type_cast(nil)).to be_nil
      end
  
      [true, 1, '1', 't', 'T', 'true', 'TRUE', 'on', 'ON'].each do |val|
        it "casts #{val.inspect} to true" do
          expect(column.type_cast(val)).to eq(true)
        end
      end
      
      [false, 0, '0', 'f', 'F', 'false', 'FALSE', 'off', 'OFF', ' ', "\u3000\r\n", "\u0000", 'SOMETHING RANDOM'].each do |val|
        it "casts #{val.inspect} to false" do
          expect(column.type_cast(val)).to eq(false)
        end
      end
    end
    
    describe "integer" do
      let(:column) {DatastaxRails::Column.new("field", nil, "integer")}
      
      [1,'1','1ignore','1.7',true].each do |val|
        it "casts #{val.inspect} to true" do
          expect(column.type_cast(val)).to eq(1)
        end
      end
      
      ['bad1','bad',false].each do |val|
        it "casts #{val.inspect} to 0" do
          expect(column.type_cast(val)).to eq(0)
        end
      end
      
      [nil,[1,2],{1 => 2},(1..2),Object.new,Float::NAN,(1.0/0.0)].each do |val|
        it "casts #{val.inspect} to nil" do
          expect(column.type_cast(val)).to be_nil
        end
      end
      
      it "casts a duration to an integer" do
        expect(column.type_cast(30.minutes)).to be(1800)
      end
    end
    
    describe "time" do
      let(:column) {DatastaxRails::Column.new("field", nil, "time")}
      
      [nil,'ABC',''].each do |val|
        it "casts #{val.inspect} to nil" do
          expect(column.type_cast(val)).to be_nil
        end
      end
      
      it "casts a time string to Time" do
        time_string = Time.now.utc.strftime("%T")
        expect(column.type_cast(time_string).strftime("%T")).to eq(time_string)
      end
    end
    
    describe "timestamp" do
      let(:column) {DatastaxRails::Column.new("field", nil, "timestamp")}
      
      [nil,'',' ', 'ABC'].each do |val|
        it "casts #{val.inspect} to nil" do
          expect(column.type_cast(val)).to be_nil
        end
      end
      
      it "casts a datetime string to Time" do
        datetime_string = Time.now.utc.strftime("%FT%T")
        expect(column.type_cast(datetime_string).strftime("%FT%T")).to eq(datetime_string)
      end
      
      it "casts a datetime string with timezone to Time" do
        begin
          old = DatastaxRails::Base.default_timezone
          [:utc, :local].each do |zone|
            ActiveRecord::Base.default_timezone = zone
            datetime_string = "Wed, 04 Sep 2013 03:00:00 EAT"
            expect(column.type_cast(val)).to eq(Time.utc(2013, 9, 4, 0, 0, 0))
          end
        rescue
          DatastaxRails::Base.default_timezone = old
        end
      end
    end
    
    describe "date" do
      let(:column) {DatastaxRails::Column.new("field", nil, "date")}
      
      [nil,'',' ', 'ABC'].each do |val|
        it "casts #{val.inspect} to nil" do
          expect(column.type_cast(val)).to be_nil
        end
      end
      
      it "casts a date string to Date" do
        date_string = Time.now.utc.strftime("%F")
        expect(column.type_cast(date_string).strftime("%F")).to eq(date_string)
      end
    end
  end
end
