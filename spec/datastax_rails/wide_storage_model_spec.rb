require 'spec_helper'

describe DatastaxRails::WideStorageModel do
  subject { build_stubbed(:audit_log) }

  context '#id_for_update' do
    subject { super().id_for_update }
    it { is_expected.to have_key('uuid') }
    it { is_expected.to have_key('created_at') }
  end
end
