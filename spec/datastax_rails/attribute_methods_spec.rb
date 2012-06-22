require 'spec_helper'

class AttributeMethodsTester < DatastaxRails::Base
  string :test_string
  string :non_search_string, :searchable => false
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
  
end
