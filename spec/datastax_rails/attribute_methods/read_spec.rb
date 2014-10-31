require 'spec_helper'

describe DatastaxRails::Base do
  context 'attribute methods' do
    context 'read' do
      context 'overrides' do
        subject { CollectionOverride.new }

        context 'lists' do
          before do
            subject.list1 = ['foo']
            subject.list2 = ['bar']
          end

          it 'returns the correct values before a save' do
            expect(subject.read_attribute(:list1)).to eq(['foo'])
            expect(subject.read_attribute(:list2)).to eq(['bar'])
            expect(subject.list1).to eq(['FOO'])
            expect(subject.list2).to eq(['BAR'])
          end

          it 'returns the correct values after a save' do
            subject.save
            expect(subject.read_attribute(:list1)).to eq(['FOO'])
            expect(subject.read_attribute(:list2)).to eq(['bar'])
            expect(subject.list1).to eq(['FOO'])
            expect(subject.list2).to eq(['BAR'])
          end
        end
      end
    end
  end
end
