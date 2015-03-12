require 'spec_helper'

describe 'DatastaxRails::Base' do
  describe 'persistence' do
    describe '#update_attributes' do
      it 'only overwrites attributes that are passed in as part of the hash' do
        person = Person.create(name: 'Jason', birthdate: Date.parse('Oct 19, 1981'), nickname: 'Jas')
        person.birthdate = Date.parse('Oct 19, 1980')
        person.update_attributes(nickname: 'Jace')
        expect(person.birthdate).to eql(Date.parse('Oct 19, 1980'))
        expect(person.nickname).to eql('Jace')
      end
    end

    describe 'with cql' do
      let(:results) { double('results', execution_info: double('EI', hosts: [double('host', ip: '127.0.0.1')])) }
      before(:each) do
        Person.storage_method = :cql
        @statement = double('prepared statement')
        allow(DatastaxRails::Base.connection).to receive(:prepare).and_return(@statement)
        allow(@statement).to receive(:bind).and_return(@statement)
      end

      describe '#create' do
        it 'should persist at the given consistency level' do
          expect(DatastaxRails::Base.connection).to receive(:execute) do |*args|
            expect(args.last).to include(consistency: :local_quorum)
            results
          end
          Person.create({ name: 'Steven' }, { consistency: 'LOCAL_QUORUM' })
        end
      end

      describe '#save' do
        it 'should persist at the given consistency level' do
          expect(DatastaxRails::Base.connection).to receive(:execute) do |*args|
            expect(args.last).to include(consistency: :local_quorum)
            results
          end
          p = Person.new(name: 'Steven')
          p.save(consistency: 'LOCAL_QUORUM')
        end
      end

      describe '#remove' do
        it 'should remove at the given consistency level' do
          allow(DatastaxRails::Base.connection).to receive(:execute).and_return(results)
          p = Person.create(name: 'Steven')
          expect(DatastaxRails::Base.connection).to receive(:execute) do |*args|
            expect(args.last).to include(consistency: :local_quorum)
            results
          end
          p.destroy(consistency: :local_quorum)
        end
      end
    end

    describe 'with solr' do
      around(:each) do |example|
        Person.storage_method = :solr
        example.run
        Person.storage_method = :cql
      end

      describe '#create' do
        it 'should persist at the given consistency level' do
          expect(Person.solr_connection).to receive(:update).with(hash_including(params: hash_including(cl: 'LOCAL_QUORUM'))).and_return(true)
          Person.create({ name: 'Steven' }, { consistency: 'LOCAL_QUORUM' })
        end
      end

      describe '#save' do
        it 'should persist at the given consistency level' do
          expect(Person.solr_connection).to receive(:update).with(hash_including(params: hash_including(cl: 'LOCAL_QUORUM'))).and_return(true)
          p = Person.new(name: 'Steven')
          p.save(consistency: 'LOCAL_QUORUM')
        end

        it 'should successfully remove columns that are set to nil' do
          Person.create!(name: 'Steven', birthdate: Date.today)
          Person.commit_solr
          p = Person.find_by(name: 'Steven')
          p.birthdate = nil
          p.save
          Person.commit_solr
          expect(Person.find_by(name: 'Steven').birthdate).to be_nil
        end

        it 'keeps existing attributes from being deleted' do
          p = Person.create!(name: 'Jacob', birthdate: Date.today)
          p.nickname = 'Jake'
          p.save
          Person.commit_solr
          p2 = Person.find_by(name: 'Jacob')
          expect(p2.name).to eql('Jacob')
          expect(p2.nickname).to eql('Jake')
          expect(p2.birthdate).to eql(Date.today)
        end
      end
    end

    describe '#store_file' do
      it 'should store a file', slow: true do
        file = 'abcd' * 1.megabyte
        digest = Digest::SHA1.hexdigest(file)
        CarPayload.create(digest: digest, payload: file)
        expect(CarPayload.find(digest).payload).to eq(file)
      end

      it 'should store really large files', slow: true do
        file = IO.read('/dev/zero', 25.megabyte)
        digest = Digest::SHA1.hexdigest(file)
        CarPayload.create(digest: digest, payload: file)
        expect(CarPayload.find(digest).payload).to eq(file)
      end

      it 'throws a ChecksumMismatchError if record incorrect' do
        file = 'abcd' * 1.megabyte
        digest = Digest::SHA1.hexdigest(file)
        CarPayload.create(digest: digest, payload: file)
        DatastaxRails::Base.connection.execute("DELETE FROM car_payloads WHERE digest='#{digest}' AND chunk=2")
        expect { CarPayload.find(digest) }.to raise_exception(DatastaxRails::ChecksumMismatchError)
      end
    end

    describe '#write_attribute' do
      let(:person) { create(:person) }

      it 'writes an attribute directly to the database' do
        Person.write_attribute(person, birthdate: Date.today)
        expect(Person.find(person.id).birthdate).to eq(Date.today)
      end
    end
  end
end
