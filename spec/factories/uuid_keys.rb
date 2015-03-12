FactoryGirl.define do
  trait(:uuid_key) do
    id { ::Cassandra::TimeUuid::Generator.new.now }
    created_at { Time.now }
  end
end
