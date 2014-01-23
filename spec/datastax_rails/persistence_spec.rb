require 'spec_helper'

describe "DatastaxRails::Base" do
  describe "persistence" do
    describe "#update_attributes" do
      it "only overwrites attributes that are passed in as part of the hash" do
        person = Person.create(:name => 'Jason', :birthdate => Date.parse("Oct 19, 1981"), :nickname => 'Jas')
        person.birthdate = Date.parse("Oct 19, 1980")
        person.update_attributes(:nickname => 'Jace')
        person.birthdate.should eql(Date.parse("Oct 19, 1980"))
        person.nickname.should eql('Jace')
      end
    end
    
    describe "with cql" do
      before(:each) do
        Person.storage_method = :cql
        @statement = double("prepared statement")
        DatastaxRails::Base.connection.stub(:prepare).and_return(@statement)
      end
      
      describe "#create" do
        it "should persist at the given consistency level" do
          @statement.should_receive(:execute).with(an_instance_of(Array), :consistency => :local_quorum)
          Person.create({:name => 'Steven'},{:consistency => 'LOCAL_QUORUM'})
        end
      end
    
      describe "#save" do
        it "should persist at the given consistency level" do
          @statement.should_receive(:execute).with(an_instance_of(Array), :consistency => :local_quorum)
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
        DatastaxRails::Base.connection.should_receive(:execute_cql_query).with(an_instance_of(String), :consistency => :local_quorum)
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
