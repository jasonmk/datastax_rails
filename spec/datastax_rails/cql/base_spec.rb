require 'spec_helper'

describe DatastaxRails::Cql::Base do
  it "caches prepared statements" do
    expect(DatastaxRails::Base.connection).to receive(:prepare).once.and_return(double("statement", :execute => true))
    cql = DatastaxRails::Cql::ColumnFamily.new(Person)
    cql.select(['*']).conditions(:name => 'John').execute
    cql.select(['*']).conditions(:name => 'John').execute
  end
end
