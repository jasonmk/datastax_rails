class Person < DatastaxRails::Base
  self.column_family = "people"
  
  has_one :job
  has_many :cars, :dependent => :destroy
  has_and_belongs_to_many :hobbies
  
  uuid :id
  text :name, :sortable => true
  date :birthdate
  string :nickname
  set :email_addresses
  map :str_
  timestamps
  
  before_create :set_variable2
  before_save :set_nickname
  after_save :set_variable
  
  validates :name, :presence => true, :uniqueness => :true
  
  def set_nickname
    self.nickname ||= self.name
  end
  
  def set_variable
    @after_save_ran = "yup"
  end
  
  def set_variable2
    @before_create_ran = "yup"
  end
end

class Car < DatastaxRails::Base
  self.column_family = "cars"
  
  belongs_to :person
  
  uuid :id
  string :name
  uuid :person_id
  uuid :car_payload_id
  datetime :last_serviced_at
  map :oil_changes, :holds => :timestamp
  timestamps
end

class CarPayload < DatastaxRails::PayloadModel
  self.column_family = "car_payloads"
end

class AuditLog < DatastaxRails::WideStorageModel
  include DatastaxRails::CassandraOnlyModel
  self.column_family = "audit_logs"
  self.primary_key = :uuid
  self.cluster_by  = :created_at
  self.create_options = 'CLUSTERING ORDER BY (created_at DESC)'
  
  uuid       :uuid
  string     :message
  string     :user, :cql_index => true
  timestamps
end

class Job < DatastaxRails::Base
  self.column_family = "jobs"
  
  belongs_to :person
  
  uuid :id
  string :title
  integer :position_number
  uuid :person_id
  list :former_positions, :holds => :integer
  timestamps
  
  validates :position_number, :uniqueness => true, :allow_blank => true
end

class Boat < DatastaxRails::Base
  self.column_family = "boats"
  
  uuid :id
  string :name
  integer :registration
  timestamps
  
  validates :name, :uniqueness => true
  default_scope order(:name)
end

class Hobby < DatastaxRails::Base
  self.column_family = "hobbies"
  
  has_and_belongs_to_many :people
  
  uuid :id
  string :name
  float :complexity
  map :components, :holds => :integer
  timestamps
end

class CoreMetadata < DatastaxRails::DynamicModel
  self.group_by = 'core'
end

class TeamMetadata < DatastaxRails::DynamicModel
  self.group_by = 'team'
end
