require 'spec_helper'

class AttributeMethodsTester < DatastaxRails::Base
  string :test_string
  string :non_search_string, :searchable => false
end 

describe DatastaxRails::Base do
  def tester
    @tester ||= AttributeMethodsTester.new
  end
  
end
