require 'spec_helper'

describe DatastaxRails::Base do
  context 'attribute methods' do
    context 'default values' do
      subject { Default.new }

      its(:str) { is_expected.to eq('string') }
      its(:bool) { is_expected.to be(true) }
      its(:bool2) { is_expected.to be(false) }
      its(:bool3) { is_expected.to be_nil }
      its(:changed_attributes) { is_expected.to include('str') }
      its(:changed_attributes) { is_expected.to include('bool') }
      its(:changed_attributes) { is_expected.to include('bool2') }
      its(:changed_attributes) { is_expected.not_to include('bool3') }

      context 'setting the attribute to the default' do
        before { subject.bool = true }

        its(:bool) { is_expected.to be(true) }
        its(:changed_attributes) { is_expected.to include('bool') }
      end

      context 'collections' do
        context 'when not specified' do
          its(:m) { is_expected.to eq({}) }
          its(:s) { is_expected.to eq(Set.new) }
          its(:l) { is_expected.to eq([]) }
        end

        context 'when specified' do
          its(:m2) { is_expected.to eq('m2test' => 'string') }
          its(:s2) { is_expected.to eq(Set.new(['unique string'])) }
          its(:l2) { is_expected.to eq(['ordered string']) }
        end
      end
    end
  end
end
