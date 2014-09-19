FactoryGirl.define do
  factory(:job) do
    uuid_key
    person
    sequence(:position_number)
  end
end
