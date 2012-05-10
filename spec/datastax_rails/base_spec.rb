require 'spec_helper'

describe DatastaxRails::Base do
  it "should run before_save" do
    p = Person.new(:name => "Jason")
    p.save
    p.nickname.should == "Jason"
  end
  
  it "should run after_save" do
    p = Person.new(:name => "Jason")
    p.save!
    p.instance_variable_get(:@after_save_ran).should == "yup"
  end
end
