require 'spec_helper'

describe "DatastaxRails::Base" do
  describe "persistence" do
    describe "#create" do
      it "should persist at the given consistency level" do
        DatastaxRails::Base.connection.stub(:execute_cql_query)
        DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(/USING CONSISTENCY LOCAL_QUORUM/i).and_return(true)
        Person.create({:name => 'Steven'},{:consistency => 'LOCAL_QUORUM'})
      end
    end
    
    describe "#save" do
      it "should persist at the given consistency level" do
        DatastaxRails::Base.connection.stub(:execute_cql_query)
        DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(/USING CONSISTENCY LOCAL_QUORUM/i).and_return(true)
        p=Person.new(:name => 'Steven')
        p.save(:consistency => 'LOCAL_QUORUM')
      end
    end
    
    describe "#remove" do
      it "should remove at the given consistency level" do
        p=Person.create(:name => 'Steven')
        DatastaxRails::Base.connection.stub(:execute_cql_query)
        DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(/USING CONSISTENCY LOCAL_QUORUM/i).and_return(true)
        p.destroy(:consistency => :local_quorum)
      end
    end
    
    describe "#store_file" do
      it "should store a file" do
        file = "abcd"*1.megabyte
        Car.create(:name => 'limo', :picture => file)
        Car.commit_solr
        Car.find_by_name('limo').picture.should == file
      end
      
      it "should successfully overwrite a larger file with a smaller one" do
        file = "abcd"*1.megabyte
        car = Car.create(:name => 'limo', :picture => file)
        Car.commit_solr
        smallfile = "e"*1.kilobyte
        car.picture = smallfile
        car.save
        Car.find_by_name('limo').picture.should == smallfile
      end
    end
  end
end
