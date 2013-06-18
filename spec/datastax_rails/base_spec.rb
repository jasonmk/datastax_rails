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
  
  it "should raise RecordNotFound when finding a bogus ID" do
    lambda { Person.find("xyzzy") }.should raise_exception(DatastaxRails::RecordNotFound)
  end
  
  xit "should skip records that are missing dsr in cassandra" do
    p = Person.create(:name => 'Jason')
    Person.cql.delete(p.id).columns(['dsr']).execute
    Person.find_by_name('Jason').should be_nil
  end
end
