require 'spec_helper'

describe DatastaxRails::Relation do
  context 'stats methods' do
    before(:all) do
      DatastaxRails::Base.recorded_classes = {}
      Hobby.create(name: 'Knitting', complexity: 1.0)
      Hobby.create(name: 'Walking', complexity: 2.0)
      Hobby.create(name: 'Skydiving', complexity: 10.0)
      Hobby.create(name: 'Flying', complexity: 50.0)
      Hobby.commit_solr
    end

    after(:all) { Hobby.truncate }

    subject { Hobby }

    context('#sum') do
      subject { super().sum(:complexity) }
      it { is_expected.to eq(63.0) }
    end

    context('#average') do
      subject { super().average(:complexity) }
      it { is_expected.to eq(15.75) }
    end

    context('#maximum') do
      subject { super().maximum(:complexity) }
      it { is_expected.to eq(50.0) }
    end

    context('#minimum') do
      subject { super().minimum(:complexity) }
      it { is_expected.to eq(1.0) }
    end

    context('#stddev') do
      subject { super().stddev(:complexity) }
      it { is_expected.to be_within(0.001).of(23.185) }
    end

    context('grouped') do
      subject { super().group(:name) }

      context('#sum') do
        subject { super().grouped_sum(:complexity) }
        it { is_expected.to eq('knitting' => 1.0, 'walking' => 2.0, 'skydiving' => 10.0, 'flying' => 50.0) }
      end

      context('#average') do
        subject { super().grouped_average(:complexity) }
        it { is_expected.to eq('knitting' => 1.0, 'walking' => 2.0, 'skydiving' => 10.0, 'flying' => 50.0) }
      end

      context('#maximum') do
        subject { super().grouped_maximum(:complexity) }
        it { is_expected.to eq('knitting' => 1.0, 'walking' => 2.0, 'skydiving' => 10.0, 'flying' => 50.0) }
      end

      context('#minimum') do
        subject { super().grouped_minimum(:complexity) }
        it { is_expected.to eq('knitting' => 1.0, 'walking' => 2.0, 'skydiving' => 10.0, 'flying' => 50.0) }
      end

      context('#stddev') do
        subject { super().grouped_stddev(:complexity) }
        it { is_expected.to eq('knitting' => 0.0, 'walking' => 0.0, 'skydiving' => 0.0, 'flying' => 0.0) }
      end
    end
  end
end
