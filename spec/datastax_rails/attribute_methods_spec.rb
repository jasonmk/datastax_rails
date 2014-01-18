require 'spec_helper'

class AttributeMethodsTester < DatastaxRails::Base
  string :test_string
  string :non_search_string, :searchable => false
end 

class CassandraOnlyTester < DatastaxRails::CassandraOnlyModel
  string :test_string
  string :test_string2, :indexed => :both
end

describe DatastaxRails::Base do
  def tester
    @tester ||= AttributeMethodsTester.new
  end
  
  describe "attribute methods" do
    it "should create attribute setter methods" do
      tester.should respond_to(:test_string=)
    end
    
    it "Should create attribute getter methods" do
      tester.should respond_to(:test_string)
    end
  end
  
  describe "#attribute" do
    context "Cassandra-only models" do
      it "does not index columns by default" do
        expect(CassandraOnlyTester.attribute_definitions[:test_string].coder.options[:indexed]).to be_false
      end
      
      it "does not index into solr" do
        expect(CassandraOnlyTester.attribute_definitions[:test_string2].coder.options[:indexed]).to eq(:cassandra)
      end
    end
    
    context "Normal models" do
      it "indexes data into solr by default" do
        expect(AttributeMethodsTester.attribute_definitions[:test_string].coder.options[:indexed]).to eq(:solr)
      end
    end
  end
  
end
