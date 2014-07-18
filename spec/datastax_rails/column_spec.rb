require 'spec_helper'

describe DatastaxRails::Column do
  describe "type casting" do
    let(:record) {double(Person, :changed_attributes => {}, :attributes => {})}
    
    describe "boolean" do
      let(:c) {DatastaxRails::Column.new("field", nil, "boolean")}
      
      describe "to ruby" do
        it "casts '' to nil" do 
          expect(c.type_cast('')).to be_nil
        end
        
        it "casts nil to nil" do
          expect(c.type_cast(nil)).to be_nil
        end
    
        [true, 1, '1', 't', 'T', 'true', 'TRUE', 'on', 'ON'].each do |val|
          it "casts #{val.inspect} to true" do
            expect(c.type_cast(val)).to eq(true)
          end
        end
        
        [false, 0, '0', 'f', 'F', 'false', 'FALSE', 'off', 'OFF', ' ', "\u3000\r\n", "\u0000", 'SOMETHING RANDOM'].each do |val|
          it "casts #{val.inspect} to false" do
            expect(c.type_cast(val)).to eq(false)
          end
        end
      end
      
      describe "to cql3" do
        it "casts false to false" do
          expect(c.type_cast_for_cql3(false)).to be_falsey
        end
        
        it "casts true to true" do
          expect(c.type_cast_for_cql3(true)).to be_truthy
        end
      end
      
      describe "to solr" do
        it "casts false to 'false'" do
          expect(c.type_cast_for_solr(false)).to eq('false')
        end
        
        it "casts true to 'true'" do
          expect(c.type_cast_for_solr(true)).to eq('true')
        end
      end
    end
    
    describe "integer" do
      let(:c) {DatastaxRails::Column.new("field", nil, "integer")}
      
      describe "to ruby" do
        [1,'1','1ignore','1.7',true].each do |val|
          it "casts #{val.inspect} to true" do
            expect(c.type_cast(val)).to eq(1)
          end
        end
        
        ['bad1','bad',false].each do |val|
          it "casts #{val.inspect} to 0" do
            expect(c.type_cast(val)).to eq(0)
          end
        end
        
        [nil,[1,2],{1 => 2},(1..2),Object.new,Float::NAN,(1.0/0.0)].each do |val|
          it "casts #{val.inspect} to nil" do
            expect(c.type_cast(val)).to be_nil
          end
        end
        
        it "casts a duration to an integer" do
          expect(c.type_cast(30.minutes)).to be(1800)
        end
      end
      
      describe "to cql3" do
        it "casts 1 to 1" do
          expect(c.type_cast_for_cql3(1)).to be(1)
        end
      end
      
      describe "to solr" do
        it "casts 1 to 1" do
          expect(c.type_cast_for_solr(1)).to be(1)
        end
      end
    end
    
    describe "time" do
      let(:c) {DatastaxRails::Column.new("field", nil, "time")}
      
      describe "to ruby" do
        [nil,'ABC',''].each do |val|
          it "casts #{val.inspect} to nil" do
            expect(c.type_cast(val)).to be_nil
          end
        end
        
        it "casts a time string to Time" do
          time_string = Time.now.utc.strftime("%T")
          expect(c.type_cast(time_string).strftime("%T")).to eq(time_string)
        end
      end
      
      describe "to cql3" do
        it "casts a Time object to a Time object" do
          time = Time.parse('1980-10-19 17:55:00')
          expect(c.type_cast_for_cql3(time)).to eq(time)
        end
      end
      
      describe "to solr" do
        it "casts a Time object to a solr formatted time string" do
          time = Time.parse('1980-10-19 17:55:00')
          expect(c.type_cast_for_solr(time)).to eq('1980-10-19T17:55:00Z')
        end
      end
    end
    
    describe "timestamp" do
      let(:c) {DatastaxRails::Column.new("field", nil, "timestamp")}
      
      describe "to ruby" do
        [nil,'',' ', 'ABC'].each do |val|
          it "casts #{val.inspect} to nil" do
            expect(c.type_cast(val)).to be_nil
          end
        end
        
        it "casts a datetime string to Time" do
          datetime_string = Time.now.utc.strftime("%FT%T")
          expect(c.type_cast(datetime_string).strftime("%FT%T")).to eq(datetime_string)
        end
        
        it "casts a datetime string with timezone to Time" do
          begin
            old = DatastaxRails::Base.default_timezone
            [:utc, :local].each do |zone|
              DatastaxRails::Base.default_timezone = zone
              datetime_string = "Wed, 04 Sep 2013 03:00:00 EAT"
              expect(c.type_cast(val)).to eq(Time.utc(2013, 9, 4, 0, 0, 0))
            end
          rescue
            DatastaxRails::Base.default_timezone = old
          end
        end
      end
      
      describe "to cql3" do
        it "casts a Time object to a Time object" do
          time = Time.parse('1980-10-19 17:55:00')
          expect(c.type_cast_for_cql3(time)).to eq(time)
        end
      end
      
      describe "to solr" do
        it "casts a Time object to a solr formatted time string" do
          time = Time.parse('1980-10-19 17:55:00')
          expect(c.type_cast_for_solr(time)).to eq('1980-10-19T17:55:00Z')
        end
      end
    end
    
    describe "date" do
      let(:c) {DatastaxRails::Column.new("field", nil, "date")}
      
      [nil,'',' ', 'ABC'].each do |val|
        it "casts #{val.inspect} to nil" do
          expect(c.type_cast(val)).to be_nil
        end
      end
      
      it "casts a date string to Date" do
        date_string = Time.now.utc.strftime("%F")
        expect(c.type_cast(date_string).strftime("%F")).to eq(date_string)
      end
      
      describe "to cql3" do
        it "casts a Date object to a Time object" do
          time = Time.parse('1980-10-19 00:00:00')
          date = Date.parse('1980-10-19')
          expect(c.type_cast_for_cql3(date)).to eq(time)
        end
      end
      
      describe "to solr" do
        it "casts a Date object to a solr formatted time string" do
          date = Date.parse('1980-10-19')
          expect(c.type_cast_for_solr(date)).to eq('1980-10-19T00:00:00Z')
        end
      end
    end
    
    describe "map" do
      let(:c) {DatastaxRails::Column.new("field_", nil, "map", :holds => :integer)}
      let(:dc) {DatastaxRails::Column.new("field_", nil, "map", :holds => :date)}
      
      it "casts map keys to strings" do
        expect(c.type_cast({:field_key => 7}, record)).to eq({"field_key" => 7})
      end
      
      it "casts map values to the type specified in the options" do
        expect(c.type_cast({'field_key' => '7'}, record)).to eq({"field_key" => 7})
      end
      
      it "wraps map values in a DynamicMap" do
        expect(c.type_cast({'field_key' => '7'}, record)).to be_a(DatastaxRails::Types::DynamicMap)
      end
      
      describe "to cql" do
        it "casts map values to the appropriate type" do
          date = Date.parse("1980-10-19")
          time = Time.parse("1980-10-19 00:00:00 +0000")
          expect(dc.type_cast_for_cql3({:field_key => date})).to eq(:field_key => time)
        end
      end
    end
    
    describe "list" do
      let(:c) {DatastaxRails::Column.new("field", nil, "list", :holds => :integer)}
      
      it "casts list values to the type specified in the options" do
        expect(c.type_cast([1,"2",6.minutes], record)).to eq([1,2,360])
      end
      
      it "wraps list values in a DynamicList" do
        expect(c.type_cast([1,"2",6.minutes], record)).to be_a(DatastaxRails::Types::DynamicList)
      end
    end
    
    describe "set" do
      let(:c) {DatastaxRails::Column.new("field", nil, "set", :holds => :integer)}
      
      it "casts list values to the type specified in the options" do
        expect(c.type_cast([1,"2",6.minutes, 2], record)).to eq(Set.new([1,2,360]))
      end
      
      it "wraps list values in a DynamicSet" do
        expect(c.type_cast([1,"2",6.minutes, 2], record)).to be_a(DatastaxRails::Types::DynamicSet)
      end
    end
  end
end
