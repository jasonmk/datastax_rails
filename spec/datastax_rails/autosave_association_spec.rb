require 'spec_helper'

describe DatastaxRails::Base do
  describe 'Autosave Associations' do
    describe 'collections' do
      it 'saves child records built via the association' do
        p = Person.new(name: 'Jim')
        c1 = p.cars.build(name: 'Jeep')
        c2 = p.cars.build(name: 'Ford')
        p.save
        Person.commit_solr
        expect(Car.find(c1.id).person).to eq(p)
        expect(Car.find(c2.id).person).to eq(p)
      end
    end
  end
end
