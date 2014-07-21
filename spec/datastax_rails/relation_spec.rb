require 'spec_helper'

describe DatastaxRails::Relation do
  before(:each) do
    @relation = DatastaxRails::Relation.new(Hobby, 'hobbies')
    @relation.default_scoped = true
    @relation.commit_solr
  end

  describe '#==' do
    it 'should count two relations with the same parameters as equal' do
      expect(@relation.where('name' => 'jason')).to eq(@relation.where('name' => 'jason'))
    end
  end

  describe '#any?' do
    it 'should return true if there are records' do
      Hobby.create(name: 'fishing')
      @relation.commit_solr
      expect(@relation.any?).to be_truthy
    end

    it 'should return false if there are no records' do
      expect(@relation.any?).to be_falsey
    end
  end

  describe '#count' do
    it 'should use the cached count if it is available' do
      @relation.instance_variable_set(:@count, 42)
      expect(@relation.count).to eq(42)
    end

    it 'should cache the total count on any solr query' do
      @relation = @relation.with_solr
      expect(@relation).to receive(:query_via_solr).and_return(double('ResultSet', total_entries: 42))
      @relation.all
      expect(@relation.count).to eq(42)
    end

    it 'should execute a fast search to determine the count' do
      mock_relation = double(DatastaxRails::Relation)
      allow(mock_relation).to receive_message_chain(:select, :to_a, :total_entries).and_return(37)
      @relation = @relation.with_solr
      expect(@relation).to receive(:limit).with(1).and_return(mock_relation)
      expect(@relation.count).to eq(37)
    end

    it 'should return the count regardless of limit' do
      Hobby.create(name: 'hiking')
      Hobby.create(name: 'boxing')
      Hobby.create(name: 'fishing')
      Hobby.create(name: 'running')
      @relation.commit_solr
      expect(@relation.count).to eq(4)

      expect(@relation.limit(2).count).to eq(4)
    end
  end

  describe '#default_scope' do
    it 'should return a relation that has no scope set' do
      Hobby.create(name: 'fishing')
      @relation.commit_solr
      relation = @relation.where('name' => 'hiking')
      expect(relation.count).to eq(0)
      expect(relation.default_scope.count).to eq(1)
    end

    it 'should return a relation that has a default scope set' do
      relation = DatastaxRails::Relation.new(Boat, 'boats')
      relation.default_scoped = true
      expect(relation.default_scope.order_values).not_to be_empty
    end
  end

  describe '#empty?' do
    it 'should use the loaded result set to determine emptiness' do
      a_record = build_stubbed(:hobby)
      allow(@relation).to receive(:loaded?).and_return(true)
      @relation.instance_variable_set(:@results, [])
      expect(@relation).to be_empty
      @relation.instance_variable_set(:@results, [a_record])
      expect(@relation).not_to be_empty
    end
  end

  describe '#many?' do
    it 'should return true if there are multiple records matching' do
      Hobby.create(name: 'hiking')
      Hobby.create(name: 'swimming')
      @relation.commit_solr
      expect(@relation).to be_many
    end

    it 'should return false if there are zero or one records matching' do
      expect(@relation).not_to be_many
      Hobby.create(name: 'hiking')
      expect(@relation).not_to be_many
    end
  end

  describe '#new' do
    it 'should instantiate a new instance of the class' do
      hiking = @relation.new(name: 'hiking')
      expect(hiking).to be_a_kind_of(Hobby)
      expect(hiking.name).to eq('hiking')
    end
  end

  describe '#reload' do
    it 'should reload the results' do
      expect(@relation.all).to be_empty
      Hobby.create(name: 'hiking')
      @relation.commit_solr
      expect(@relation.all).to be_empty
      expect(@relation.reload.all).not_to be_empty
    end
  end

  describe '#size' do
    it 'should return the size of the current result set (including limit setting)' do
      Hobby.create(name: 'hiking')
      Hobby.create(name: 'boxing')
      Hobby.create(name: 'fishing')
      Hobby.create(name: 'running')
      @relation.commit_solr
      expect(@relation.size).to eq(4)
      expect(@relation.limit(2).size).to eq(2)
    end
  end

  describe '#total_pages' do
    it 'should calculate the total number of pages for will_paginate' do
      relation = @relation.per_page(30)
      allow(relation).to receive(:count).and_return(100)
      expect(relation.total_pages).to eq(4)
    end
  end

  describe 'grouped queries' do
    before(:each) do
      Person.commit_solr
      Person.create(name: 'John', nickname: 'J')
      Person.create(name: 'Jason', nickname: 'J')
      Person.create(name: 'James', nickname: 'J')
      Person.create(name: 'Kathrine', nickname: 'Kat')
      Person.create(name: 'Kathy', nickname: 'Kat')
      Person.create(name: 'Steven', nickname: 'Steve')
      Person.commit_solr
    end

    it 'should return matching documents grouped by an attribute' do
      results = Person.group(:nickname).all
      expect(results['j'].size).to eq(3)
      expect(results['kat'].size).to eq(2)
      expect(results['steve'].size).to eq(1)
    end

    it 'should return total_entries as the highest value of any group' do
      results = Person.group(:nickname).all
      expect(results.total_entries).to eq(3)
    end

    it 'should still return a total count when using the count method' do
      expect(Person.group(:nickname).count).to eq(6)
    end
  end

  describe '#downcase_query' do
    it 'downcases a solr query while leaving operators untouched' do
      solr_query = 'This Query needs to be DOWNCASED AND it also searches DATES ' \
                   '[2010-09-09T10:42:12Z TO 2011-08-08T09:23:34Z] OR maybe it just breaks'
      expect(@relation.downcase_query(solr_query)).to eq('this query needs to be downcased AND it also searches ' \
                                                         'dates [2010-09-09T10:42:12Z TO 2011-08-08T09:23:34Z] ' \
                                                         'OR maybe it just breaks')
    end
  end
end
