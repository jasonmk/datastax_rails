FactoryGirl.define do
  factory(:person_role) do
    person
    role
    uuid_key
  end
end
