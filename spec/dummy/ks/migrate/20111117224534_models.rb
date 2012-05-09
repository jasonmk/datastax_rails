class Models < CassandraObject::Schema::Migration
  MODELS = [:people, :cars, :jobs, :boats, :hobbies]
  
  def self.up
    MODELS.each do |m|
      create_column_family m do |cf|
        cf.comment = m.to_s.titleize
        cf.comparator_type = :string
        cf.column_type = 'Standard'
      end
    end
  end

  def self.down
    MODELS.each do |m|
      drop_column_family m
    end
  end

end
