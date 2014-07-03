require 'spec_helper'

describe DatastaxRails::Base do
  it "clears the statement cache when a new connection is established" do
    DatastaxRails::Base.statement_cache[:a] = 12345
    DatastaxRails::Base.establish_connection(DatastaxRails::Base.config)
    expect(DatastaxRails::Base.statement_cache).to be_empty
  end
end
