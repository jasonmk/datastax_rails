FactoryGirl.define do
  factory(:audit_log) do
    message 'Updated something'
    user 'jimbob'
  end
end
