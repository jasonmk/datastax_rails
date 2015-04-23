require 'spec_helper'

describe DatastaxRails::LoadBalancing::Policies::StickyDcAwareRoundRobin do
  let(:host1) { Cassandra::Host.new(IPAddr.new('127.0.0.1'), Cassandra::TimeUuid::Generator.new.now, 'r1', 'DC1', '2.0.14.348', '0') }
  let(:host2) { Cassandra::Host.new(IPAddr.new('127.0.0.2'), Cassandra::TimeUuid::Generator.new.now, 'r1', 'DC1', '2.0.14.348', '1') }
  let(:host3) { Cassandra::Host.new(IPAddr.new('127.0.0.3'), Cassandra::TimeUuid::Generator.new.now, 'r1', 'DC1', '2.0.14.348', '2') }
  let(:options) { Cassandra::Execution::Options.new(consistency: :quorum) }

  subject { described_class.new(500) }

  before(:each) do
    subject.host_up(host1)
    subject.host_up(host2)
    subject.host_up(host3)
  end

  it 'returns a Plan that sticks to a host' do
    host = subject.plan('system', 'select count(*) from schema_keyspaces', options).next
    499.times do
      expect(subject.plan('system', 'select count(*) from schema_keyspaces', options).next).to eq(host)
    end
    expect(subject.plan('system', 'select count(*) from schema_keyspaces', options).next).not_to eq(host)
  end

  it 'rolls to a new node if one goes down' do
    host = subject.plan('system', 'select count(*) from schema_keyspaces', options).next
    10.times do
      expect(subject.plan('system', 'select count(*) from schema_keyspaces', options).next).to eq(host)
    end
    subject.host_down(host)
    expect(subject.plan('system', 'select count(*) from schema_keyspaces', options).next).not_to eq(host)
  end
end
