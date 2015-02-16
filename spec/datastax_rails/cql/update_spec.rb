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

  it 'supports TTL updates' do
    cql = DatastaxRails::Cql::Update.new(model_class, 'id' => '12345')
    cql.columns(name: 'Steve').ttl(1.day)
    expect(cql.to_cql).to eql('update users USING TTL 86400 SET "name" = ? WHERE "id" = ?')
  end
  
  it 'supports timestamps' do
    ts = 1424113493904139
    cql = DatastaxRails::Cql::Update.new(model_class, 'id' => '12345')
    cql.columns(name: 'Steve').timestamp(ts)
    expect(cql.to_cql).to eql('update users USING TIMESTAMP 1424113493904139 SET "name" = ? WHERE "id" = ?')
  end
  
  it 'supports TTLs and timestamps together' do
    ts = 1424113493904139
    cql = DatastaxRails::Cql::Update.new(model_class, 'id' => '12345')
    cql.columns(name: 'Steve').ttl(1.day).timestamp(ts)
    expect(cql.to_cql).to eql('update users USING TTL 86400 AND TIMESTAMP 1424113493904139 SET "name" = ? WHERE "id" = ?')
  end

  it_has_behavior 'default_consistency'
end
