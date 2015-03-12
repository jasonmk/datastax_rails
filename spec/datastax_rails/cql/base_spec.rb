require 'spec_helper'

describe DatastaxRails::Cql::Base do
  let(:results) { double('results', execution_info: double('EI', hosts: [double('host', ip: '127.0.0.1')])) }
  it 'caches prepared statements' do
    statement = double('statement')
    allow(statement).to receive(:bind).and_return(statement)
    expect(DatastaxRails::Base.connection).to receive(:prepare).once.and_return(statement)
    allow(DatastaxRails::Base.connection).to receive(:execute).and_return(results)

    cql = DatastaxRails::Cql::ColumnFamily.new(Person)
    cql.select(['*']).conditions(name: 'John').execute
    cql.select(['*']).conditions(name: 'John').execute
  end
end
