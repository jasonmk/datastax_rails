require 'spec_helper'

describe DatastaxRails::Base do
  context 'associations' do
    context 'collection' do
      subject { Person.new(name: 'Matthew') }

      describe '#size' do
        it 'reports the total of persisted and non-persisted' do
          subject.save
          Car.create(person_id: subject.id)
          car2 = Car.new
          subject.cars << car2
          Car.commit_solr
          expect(subject.cars.size).to be(2)
        end
      end

      describe '#empty?' do
        it 'returns false if a new record exists' do
          subject.cars << Car.new
          expect(subject.cars).not_to be_empty
        end
      end

      describe '#any?' do
        it 'returns true if a new record exists' do
          subject.cars << Car.new
          expect(subject.cars).to be_any
        end
      end

      describe '#many?' do
        it 'returns false if more than one new record exists' do
          subject.cars << Car.new
          subject.cars << Car.new
          expect(subject.cars).to be_many
        end
      end
    end
  end
end
