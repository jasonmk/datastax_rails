FactoryGirl.define do
  trait(:uuid_key) do
    id { Cql::TimeUuid::Generator.new.next }
    created_at { Time.now }
  end
end
