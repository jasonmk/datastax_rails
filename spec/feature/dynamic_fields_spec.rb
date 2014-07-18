require 'spec_helper'

feature "Dynamic Fields" do
  scenario "map entries dynamically populate solr fields" do
    Person.create!(:name => 'Steve', :str_ => {'str_favorite_color' => 'blue'})
    Person.commit_solr
    expect(Person.where(:str_favorite_color => 'blue').entries.size).to eq(1)
  end
end
