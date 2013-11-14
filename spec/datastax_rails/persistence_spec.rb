require 'spec_helper'

describe "DatastaxRails::Base" do
  describe "persistence" do
    describe "with cql" do
      describe "#create" do
        it "should persist at the given consistency level" do
          Person.storage_method = :cql
          DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(an_instance_of(String), :consistency => CassandraCQL::Thrift::ConsistencyLevel::LOCAL_QUORUM)
          Person.create({:name => 'Steven'},{:consistency => 'LOCAL_QUORUM'})
        end
      end
    
      describe "#save" do
        it "should persist at the given consistency level" do
          Person.storage_method = :cql
          DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(an_instance_of(String), :consistency => CassandraCQL::Thrift::ConsistencyLevel::LOCAL_QUORUM)
          p=Person.new(:name => 'Steven')
          p.save(:consistency => 'LOCAL_QUORUM')
        end
      end
    end

    describe "with solr" do
      describe "#create" do
        it "should persist at the given consistency level" do
          Person.solr_connection.should_receive(:update).with(hash_including(:params => hash_including({:cl => 'LOCAL_QUORUM'}))).and_return(true)
          Person.storage_method = :solr
          Person.create({:name => 'Steven'},{:consistency => 'LOCAL_QUORUM'})
        end
      end
    
      describe "#save" do
        it "should persist at the given consistency level" do
          Person.solr_connection.should_receive(:update).with(hash_including(:params => hash_including({:cl => 'LOCAL_QUORUM'}))).and_return(true)
          Person.storage_method = :solr
          p=Person.new(:name => 'Steven')
          p.save(:consistency => 'LOCAL_QUORUM')
        end
        
        it "should successfully remove columns that are set to nil" do
          pending do 
            Person.storage_method = :solr
            p = Person.create!(:name => 'Steven', :birthdate => Date.today)
            Person.commit_solr
            p = Person.find_by_name('Steven')
            p.birthdate = nil
            p.save
            Person.commit_solr
            Person.find by_name('Steven').birthdate.should be_nil
          end
        end
      end
    end
    
    describe "#remove" do
      it "should remove at the given consistency level" do
        p=Person.create(:name => 'Steven')
        DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(an_instance_of(String), :consistency => CassandraCQL::Thrift::ConsistencyLevel::LOCAL_QUORUM)
        p.destroy(:consistency => :local_quorum)
      end
    end
    
    describe "#store_file" do
      it "should store a file", :slow => true do
        file = "abcd"*1.megabyte
        CarPayload.create(:digest => 'limo', :payload => file)
        CarPayload.find('limo').payload.should == file
      end
      
      it "should store really large files", :slow => true do
        file = IO.read("/dev/zero", 25.megabyte)
        CarPayload.create(:digest => 'limo', :payload => file)
        CarPayload.find('limo').payload.should == file
      end
      
      it "should successfully overwrite a larger file with a smaller one", :slow => true do
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
