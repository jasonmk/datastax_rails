require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, 'hobbies')
  end

  describe '#consistency' do
    it 'should throw an ArgumentError for invalid consistency levels' do
      expect { @relation.consistency(:foo) }.to raise_exception(ArgumentError)
    end

    it 'should not raise an exception for a valid consistency level' do
      expect { @relation.consistency(:local_quorum) }.not_to raise_exception
    end

    it 'should call cassandra to enforce consistency' do
      h = Hobby.create(name: 'swimming')
      Hobby.commit_solr
      allow(Hobby).to receive_message_chain(:with_cassandra, :consistency).and_return(@relation)
      expect(@relation).to receive(:find_by_id).with(h.id)
      @relation.consistency(:all).where(name: 'swimming').all
    end
  end

  describe '#limit' do
    it 'should limit the page size' do
      'a'.upto('l') do |letter|
        Hobby.create(name: letter)
      end
      Hobby.commit_solr
      expect(@relation.limit(7).all.size).to eq(7)
    end
  end

  describe '#page' do
    it 'should get a particular page' do
      'a'.upto('l') do |letter|
        Hobby.create(name: letter)
      end
      Hobby.commit_solr
      expect(@relation.per_page(3).page(2).order(:name).all.first.name).to eq('d')
    end
  end

  describe '#group' do
  end

  describe '#order' do
    it 'should return items in ascending order' do
      %w(fishing hiking boating jogging swimming chess).each do |word|
        Hobby.create(name: word)
      end
      @relation.commit_solr
      expect(@relation.order(:name).map(&:name)).to eq(%w(boating chess fishing hiking jogging swimming))
    end

    it 'should return items in descending order' do
      %w(fishing hiking boating jogging swimming chess).each do |word|
        Hobby.create!(name: word)
      end
      @relation.commit_solr
      expect(@relation.order(name: :desc).map(&:name)).to eq(%w(swimming jogging hiking fishing chess boating))
    end
  end

  describe '#select' do
    it 'returns maps from solr automatically' do
      Hobby.create!(name: 'legos', components: { 'squares' => 4, 'rectangles' => 6 })
      @relation.commit_solr
      expect(@relation.select(:components).with_solr.first.components).to have_key('squares')
    end
  end

  describe '#slow_order' do
    it 'should manually order items coming from Cassandra' do
      %w(john jason michael tony billy jim phil).each_with_index do |name, i|
        AuditLog.create!(uuid: "c1401540-f092-11e2-9001-6a5ab73a986#{i}", user: name, message: 'changed')
      end
      expect(AuditLog.unscoped.slow_order(user: :asc).map(&:user)).to eq(%w(billy jason jim john michael phil tony))
    end

    it 'should manually order items coming from Cassandra in descending order' do
      %w(john jason michael tony billy jim phil).each_with_index do |name, i|
        AuditLog.create!(uuid: "c1401540-f092-11e2-9001-6a5ab73a986#{i}", user: name, message: 'changed')
      end
      expect(AuditLog.unscoped.slow_order(user: :desc).map(&:user)).to eq(%w(tony phil michael john jim jason billy))
    end
  end

  describe '#where' do
    it 'should return documents where a field is nil (does not exist)' do
      Hobby.create(name: 'Swimming')
      Hobby.create(name: nil)
      @relation.commit_solr
      expect(@relation.where(name: nil)).not_to be_empty
    end

    it 'should return documents with false' do
      Default.create
      Default.commit_solr
      expect(Default.where(bool2: false)).not_to be_empty
    end

    it 'should not return documents with nil booleans' do
      Default.create
      Default.commit_solr
      expect(Default.where(bool3: false)).to be_empty
    end

    it 'should return documents where a value is greater than the given value' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where(:complexity).greater_than(1.0)).not_to be_empty
    end

    it 'should allow :greater_than to be specified in a single call' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where(complexity: { greater_than: 1.0 })).not_to be_empty
    end

    it 'should return documents where a value is less than the given value' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where(:complexity).less_than(2.0)).not_to be_empty
    end

    it 'should allow :less_than to be specified in a single call' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where(complexity: { less_than: 2.0 })).not_to be_empty
    end

    it 'should handle negative numbers without breaking' do
      Hobby.create(name: 'jogging', complexity: -1.2)
      @relation.commit_solr
      expect(@relation.where(:complexity).less_than(-1)).not_to be_empty
    end

    it 'should not tokenize where queries on spaces' do
      Hobby.create(name: 'horseback riding')
      @relation.commit_solr
      expect(@relation.where(name: 'horseback')).to be_empty
      expect(@relation.where(name: 'horseback riding')).not_to be_empty
      expect(@relation.where(name: 'horseback ri*')).not_to be_empty
    end

    it 'should not tokenize where queries on spaces inside arrays' do
      Hobby.create(name: 'horseback riding')
      @relation.commit_solr
      expect(@relation.where(name: ['horseback riding', 'some other hobby'])).not_to be_empty
    end

    it 'should search for values within a range' do
      Hobby.create(name: 'jogging', complexity: 1.2)
      @relation.commit_solr
      expect(@relation.where(complexity: 1..2)).not_to be_empty
      expect(@relation.where(complexity: 2..3)).to be_empty
    end

    context 'with an array as a parameter' do
      it 'becomes an OR query' do
        %w(fishing hiking boating jogging swimming chess).each do |word|
          Hobby.create(name: word)
        end
        @relation.commit_solr
        expect(@relation.where(name: %w(boating jogging chess skydiving)).size).to eq(3)
      end

      context 'against the primary key' do
        it 'removes nil values' do
          hobby = create(:hobby)
          expect(@relation.where(id: [hobby.id, nil])).to eq([hobby])
        end

        it 'returns an empty array on only nil values' do
          expect(@relation.where(id: [nil, nil])).to be_empty
        end
      end
    end
  end

  describe '#where_not' do
    it 'should return documents where a field has any value' do
      Hobby.create(name: 'Swimming')
      @relation.commit_solr
      expect(@relation.where_not(name: nil)).not_to be_empty
    end

    it 'should return documents where none of the options are present' do
      Hobby.create(name: 'Swimming')
      Hobby.create(name: 'Biking')
      @relation.commit_solr
      expect(@relation.where_not(name: %w(Swimming Biking))).to be_empty
    end

    it 'should return documents where a value is not greater than the given value' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where_not(:complexity).greater_than(2.0)).not_to be_empty
    end

    it 'should allow :greater_than to be specified in a single call' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where_not(complexity: { greater_than: 2.0 })).not_to be_empty
    end

    it 'should return documents where a value is not less than the given value' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where_not(:complexity).less_than(1.0)).not_to be_empty
    end

    it 'should allow :less_than to be specified in a single call' do
      Hobby.create(name: 'Swimming', complexity: 1.1)
      @relation.commit_solr
      expect(@relation.where_not(complexity: { less_than: 1.0 })).not_to be_empty
    end

    it 'should search for values outside a range' do
      Hobby.create(name: 'jogging', complexity: 1.2)
      @relation.commit_solr
      expect(@relation.where_not(complexity: 1..2)).to be_empty
      expect(@relation.where_not(complexity: 2..3)).not_to be_empty
    end
  end

  describe '#fulltext' do
    it 'should allow case-insensitive wildcard searches' do
      Hobby.create(name: 'Swimming')
      @relation.commit_solr
      expect(@relation.fulltext('swimming')).not_to be_empty
    end
  end

  describe '#highlight' do
    let(:hl) { @relation.highlight(:name, :description, snippet: 3, fragsize: 200) }

    it { expect(hl.highlight_options[:fields]).to eq [:name, :description] }
    it { expect(hl.highlight_options[:snippet]).to eq 3 }
    it { expect(hl.highlight_options[:fragsize]).to eq 200 }

    context 'with duplicate fields' do
      let(:hl) { @relation.highlight(:name, :description, :name) }

      it { expect(hl.highlight_options[:fields]).to eq [:name, :description] }
    end
  end

  describe '#solr_format' do
    context 'when formatting Time' do
      let(:time) { Time.new 2011, 10, 9, 8, 7, 6, '-05:00' }
      let(:c) { DatastaxRails::Column.new('field', nil, 'time') }

      it { expect(@relation.solr_format(c, time)).to eq '2011-10-09T13:07:06Z' }
    end

    context 'when formatting Date' do
      let(:date) { Date.new 2001, 2, 3 }
      let(:c) { DatastaxRails::Column.new('field', nil, 'date') }

      it { expect(@relation.solr_format(c, date)).to eq '2001-02-03T00:00:00Z' }
    end

    context 'when formatting DateTime' do
      let(:datetime) { DateTime.new 2001, 2, 3, 4, 5, 6, '-07:00' }
      let(:c) { DatastaxRails::Column.new('field', nil, 'timestamp') }

      it { expect(@relation.solr_format(c, datetime)).to eq '2001-02-03T11:05:06Z' }
    end
  end
end
