require 'spec_helper'

describe "DatastaxRails::Base" do
  describe "persistence" do
    describe "with cql" do
      describe "#create" do
        it "should persist at the given consistency level" do
          DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(/USING CONSISTENCY LOCAL_QUORUM/i).and_return(true)
          Person.storage_method = :cql
          Person.create({:name => 'Steven'},{:consistency => 'LOCAL_QUORUM'})
        end
      end
    
      describe "#save" do
        it "should persist at the given consistency level" do
          DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(/USING CONSISTENCY LOCAL_QUORUM/i).and_return(true)
          Person.storage_method = :cql
          p=Person.new(:name => 'Steven')
          p.save(:consistency => 'LOCAL_QUORUM')
        end
      end
    end

    describe "with solr" do
      describe "#create" do
        it "should persist at the given consistency level" do
          Person.solr_connection.should_receive(:update).with(hash_including(:params => {:replacefields => false, :cl => 'LOCAL_QUORUM'})).and_return(true)
          Person.storage_method = :solr
          Person.create({:name => 'Steven'},{:consistency => 'LOCAL_QUORUM'})
        end
      end
    
      describe "#save" do
        it "should persist at the given consistency level" do
          Person.solr_connection.should_receive(:update).with(hash_including(:params => {:replacefields => false, :cl => 'LOCAL_QUORUM'})).and_return(true)
          Person.storage_method = :solr
          p=Person.new(:name => 'Steven')
          p.save(:consistency => 'LOCAL_QUORUM')
        end
      end
    end
    
    describe "#remove" do
      it "should remove at the given consistency level" do
        p=Person.create(:name => 'Steven')
        DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(/USING CONSISTENCY LOCAL_QUORUM/i).and_return(true)
        p.destroy(:consistency => :local_quorum)
      end
    end
    
    describe "#store_file" do
      it "should store a file" do
        file = "abcd"*1.megabyte
        CarPayload.create(:digest => 'limo', :payload => file)
        CarPayload.find('limo').payload.should == file
      end
      
      it "should store really large files" do
        file = IO.read("/dev/urandom", 25.megabyte)
        CarPayload.create(:digest => 'limo', :payload => file)
        CarPayload.find('limo').payload.should == file
      end
      
      it "should successfully overwrite a larger file with a smaller one" do
        file = "abcd"*1.megabyte
        car = CarPayload.create(:digest => 'limo', :payload => file)
        smallfile = "e"*1.kilobyte
        car.payload = smallfile
        car.save
        CarPayload.find('limo').payload.should == smallfile
      end
    end
  end
end
