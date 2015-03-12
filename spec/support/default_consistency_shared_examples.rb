require 'spec_helper'

shared_examples_for 'default_consistency' do
  let(:prepared_statement) { double('PreparedStatement', bind: nil) }
  let(:results) { double('results', execution_info: double('EI', hosts: [double('host', ip: '127.0.0.1')])) }
  before { allow(DatastaxRails::Base.connection).to receive(:prepare).and_return(prepared_statement) }

  it 'should default to QUORUM' do
    cql = DatastaxRails::Cql::Select.new(model_class, ['*'])
    expect(DatastaxRails::Base.connection).to receive(:execute).with(nil, consistency: :quorum)
      .and_return(results)
    cql.execute
  end

  it 'should default to level specified by model class' do
    allow(model_class).to receive(:default_consistency).and_return('LOCAL_QUORUM')
    cql = DatastaxRails::Cql::Select.new(model_class, ['*'])
    expect(DatastaxRails::Base.connection).to receive(:execute).with(nil, consistency: :local_quorum)
      .and_return(results)
    cql.execute
  end
end
