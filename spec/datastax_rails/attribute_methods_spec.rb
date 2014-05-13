require 'spec_helper'

class AttributeMethodsTester < DatastaxRails::Base
  string :test_string
  string :non_search_string, :solr_index => false
end 

class CassandraOnlyTester < DatastaxRails::Base
  include DatastaxRails::CassandraOnlyModel
  string :test_string
  string :test_string2, :cql_index => true
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
        expect(CassandraOnlyTester.attribute_definitions[:test_string].options[:solr_index]).to be_false
        expect(CassandraOnlyTester.attribute_definitions[:test_string].options[:cql_index]).to be_false
      end
      
      it "does not index into solr" do
        expect(CassandraOnlyTester.attribute_definitions[:test_string2].options[:solr_index]).to be_false
        expect(CassandraOnlyTester.attribute_definitions[:test_string2].options[:cql_index]).to be_true
      end
    end
    
    context "Normal models" do
      it "indexes data into solr by default" do
        expect(AttributeMethodsTester.attribute_definitions[:test_string].options[:solr_index]).to be_true
      end
    end
  end
  
end
