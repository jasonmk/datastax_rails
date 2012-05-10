require 'spec_helper'

class AttributeMethodsTester < DatastaxRails::Base
  string :test_string
  string :non_search_string, :searchable => false
end 

describe DatastaxRails::Base do
  def tester
    @tester ||= AttributeMethodsTester.new
  end
  
  it "should automatically declare string fields as searchable" do
    Sunspot::Setup.instance_variable_get(:@setups)[:AttributeMethodsTester].field(:test_string).should be_an_instance_of(Sunspot::AttributeField)
  end
  
  it "should not declare fields as searchable if told not to" do
    lambda { Sunspot::Setup.instance_variable_get(:@setups)[:AttributeMethodsTester].field(:non_search_string) }.should raise_exception(Sunspot::UnrecognizedFieldError)
  end
  
  it "should add unqiue_id to the Sunspot config automatically" do
    Sunspot::Setup.for(AttributeMethodsTester).fields.detect {|f|f.name == :unique_id}.should_not be_nil
  end
end
