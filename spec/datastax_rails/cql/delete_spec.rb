require 'spec_helper'

describe DatastaxRails::Cql::Delete do
  context 'with single primary key' do
    it 'should generate valid CQL' do
      cql = DatastaxRails::Cql::Delete.new(Person, id: '12345')
      expect(cql.to_cql).to match(/DELETE\s+FROM people WHERE "id" = \?/)
      expect(cql.instance_variable_get(:@values)).to eq(['12345'])
    end
  end

  context 'with compound primary key' do
    it 'should generate valid CQL' do
      cql = DatastaxRails::Cql::Delete.new(CoreMetadata, id: '12345', group: 'core')
      expect(cql.to_cql).to match(/DELETE\s+FROM dynamic_model WHERE "id" = \? AND "group" = \?/)
      expect(cql.instance_variable_get(:@values)).to eq(%w(12345 core))
    end
  end
end
