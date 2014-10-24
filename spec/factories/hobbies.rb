FactoryGirl.define do
  factory(:hobby) do
    uuid_key
    name 'Biking'
    complexity 2.0
  end
end
