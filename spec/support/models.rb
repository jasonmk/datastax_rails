class Person < DatastaxRails::Base
  self.column_family = 'people'

  has_one :job
  has_many :cars, dependent: :destroy
  has_and_belongs_to_many :hobbies

  uuid :id
  text :name, sortable: true
  date :birthdate
  string :nickname
  set :email_addresses
  map :str_
  timestamps

  validates :name, presence: true, uniqueness: :true
end

class Car < DatastaxRails::Base
  self.column_family = 'cars'

  belongs_to :person

  uuid :id
  string :name
  uuid :person_id
  uuid :car_payload_id
  datetime :last_serviced_at
  map :oil_changes, holds: :timestamp
  timestamps
end

class CarPayload < DatastaxRails::PayloadModel
  self.column_family = 'car_payloads'
end

class AuditLog < DatastaxRails::WideStorageModel
  include DatastaxRails::CassandraOnlyModel
  self.column_family = 'audit_logs'
  self.primary_key = :uuid
  self.cluster_by  = :created_at
  self.create_options = 'CLUSTERING ORDER BY (created_at DESC)'

  uuid :uuid
  string :message
  string :user, cql_index: true
  timestamps
end

class Job < DatastaxRails::Base
  self.column_family = 'jobs'

  belongs_to :person

  uuid :id
  string :title
  integer :position_number
  uuid :person_id
  list :former_positions, holds: :integer
  timestamps

  validates :position_number, uniqueness: true, allow_blank: true
end

class Boat < DatastaxRails::Base
  self.column_family = 'boats'

  uuid :id
  string :name
  integer :registration
  timestamps

  validates :name, uniqueness: true
  default_scope order(:name)
end

class Hobby < DatastaxRails::Base
  self.column_family = 'hobbies'

  has_and_belongs_to_many :people

  uuid :id
  string :name
  float :complexity
  map :components, holds: :integer
  timestamps
end

class Default < DatastaxRails::Base
  self.column_family = 'defaults'

  uuid :id
  string :str, default: 'string'
  boolean :bool, default: true
  boolean :bool2, default: false
  boolean :bool3
  integer :version, default: 1
  float :complexity, default: 0.0
  uuid :previous_id, default: '00000000-0000-0000-0000-000000000000'
  date :epoch, default: Date.parse('1970-01-01')
  datetime :epoch2, default: Time.parse('1970-01-01 00:00:00')
  map :m
  map :m2, default: { 'test' => 'string' }
  set :s
  set :s2, default: ['unique string']
  list :l
  list :l2, default: ['ordered string']
end

class CoreMetadata < DatastaxRails::DynamicModel
  self.grouping = 'core'
end

class TeamMetadata < DatastaxRails::DynamicModel
  self.grouping = 'team'
end
