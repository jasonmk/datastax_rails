# This spec is used for association tests that don't necessarily fit in a single type
require 'spec_helper'

describe DatastaxRails::Base do
  context 'associations' do
    context 'many-to-many joins' do
      let(:person) { create(:person) }
      let(:role) { create(:role) }
      let(:role2) { create(:role, name: 'GUEST') }

      before do
        create(:person_role, person: person, role: role)
        create(:person_role, person: person, role: role2)
        Person.commit_solr
        Role.commit_solr
        PersonRole.commit_solr
        DatastaxRails::Base.log_solr_queries = true
        DatastaxRails::Base.log_cql_queries = true
      end

      it 'collects the far-side records' do
        expect(Role.find(person.person_roles.map(&:role_id)).compact).to eq([role, role2])
      end
    end
  end
end
