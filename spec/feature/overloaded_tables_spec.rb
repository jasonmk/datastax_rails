require 'spec_helper'

feature 'Table Overloads' do
  scenario 'models writing to the same column family do not collide' do
    core_meta = CoreMetadata.new
    core_meta.strings[:source] = 'BBC'
    core_meta.strings[:author] = 'John'
    core_meta.timestamps[:published_at] = Time.now
    core_meta.save

    team_meta = TeamMetadata.new
    team_meta.id = core_meta.id
    team_meta.strings[:source] = 'TV'
    team_meta.strings[:medium] = 'television'
    team_meta.dates[:published_on] = Date.today
    team_meta.save

    CoreMetadata.commit_solr

    expect(CoreMetadata.fulltext('BBC').entries.size).to eq(1)
    expect(CoreMetadata.fulltext('TV').entries.size).to eq(0)
  end
end
