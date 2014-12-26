require 'spec_helper'

describe DatastaxRails::Cql::Update do
  let(:model_class) { double('Model Class', column_family: 'users', default_consistency: DatastaxRails::Cql::Consistency::QUORUM, primary_key: 'id') }

  it 'should generate valid CQL' do
    cql = DatastaxRails::Cql::Update.new(model_class, 'id' => '12345')
    cql.using(DatastaxRails::Cql::Consistency::QUORUM).columns(name: 'John', age: '23')
    expect(cql.to_cql).to match(/update users SET ("name" = \?, "age" = \?|"age" = \?, "name" = \?) WHERE "id" = \?/)
  end

  it 'supports lightweight transactions' do
    cql = DatastaxRails::Cql::Update.new(model_class, 'id' => '12345')
    cql.columns(age: '25').iff(name: 'John')
    expect(cql.to_cql).to eql('update users SET "age" = ? WHERE "id" = ? IF "name" = ?')
  end

  it_has_behavior 'default_consistency'
end
