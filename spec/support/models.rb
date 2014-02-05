class Person < DatastaxRails::Base
  self.column_family = "people"
  self.legacy_mapping = true
  
  has_one :job
  has_many :cars, :dependent => :destroy
  has_and_belongs_to_many :hobbies
  
  key :uuid
  text :name, :sortable => true
  date :birthdate
  string :nickname
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
  self.legacy_mapping = true
  
  belongs_to :person
  
  key :uuid
  string :name
  string :person_id
  string :car_payload_id
  time :last_serviced_at
  timestamps
end

class CarPayload < DatastaxRails::PayloadModel
  self.column_family = "car_payloads"
end

class AuditLog < DatastaxRails::WideStorageModel
  self.column_family = "audit_logs"
  
  key :natural, :attributes => [:uuid]
  cluster_by :created_at
  
  string     :uuid
  string     :message
  string     :user, :indexed => :cassandra
  timestamps
end

class Job < DatastaxRails::Base
  self.column_family = "jobs"
  self.legacy_mapping = true
  
  belongs_to :person
  
  key :uuid
  string :title
  integer :position_number
  string :person_id
  timestamps
  
  validates :position_number, :uniqueness => true, :allow_blank => true
end

class Boat < DatastaxRails::Base
  self.column_family = "boats"
  self.legacy_mapping = true
  
  key :uuid
  string :name
  integer :registration
  timestamps
  
  validates :name, :uniqueness => true
  default_scope order(:name)
end

class Hobby < DatastaxRails::Base
  self.column_family = "hobbies"
  self.legacy_mapping = true
  
  has_and_belongs_to_many :people
  
  key :uuid
  string :name
  float :complexity
  timestamps
end
