require 'spec_helper'

describe DatastaxRails::Types::DynamicSet do
  let(:set) { described_class.new(double('record', changed_attributes: {}, attributes: {}), 'set', []) }
  subject { set }
  before(:each) do
    set << 'Test String 1'
    set << 'Another Test String'
    set.add('Test String 1')
    set << nil
  end

  it { is_expected.to eq(Set.new(['Test String 1', 'Another Test String', nil])) }
  its('record.changed_attributes') { is_expected.to include('set' => Set.new) }
  its('record.attributes') { is_expected.to include('set' => Set.new(['Test String 1', 'Another Test String', nil])) }

  context 'updating an existing record' do
    subject { FactoryGirl.build_stubbed(:person, email_addresses: Set.new(['john@example.com'])) }

    before do
      subject.changed_attributes.clear
      subject.email_addresses << 'john@compuserve.com'
    end

    it { is_expected.to be_changed }
    its(:changed_attributes) { is_expected.to include('email_addresses' => Set.new(['john@example.com'])) }
  end
end
