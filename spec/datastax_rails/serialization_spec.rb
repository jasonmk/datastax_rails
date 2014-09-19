require 'spec_helper'

describe DatastaxRails::Base do
  describe 'Serialization' do
    context '#serializable_hash' do
      subject { build_stubbed(:job) }

      it 'converts Cql::UUIDs to strings' do
        expect(subject.serializable_hash['id']).to be_a(String)
      end

      it 'converts nested Cql::UUIDs to strings' do
        expect(subject.serializable_hash(include: :person)['person']['id']).to be_a(String)
      end
    end
  end
end
