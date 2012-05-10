module DatastaxRails
  class RecordInvalid < StandardError
    attr_reader :record
    def initialize(record)
      @record = record
      super("Invalid record: #{@record.errors.full_messages.to_sentence}")
    end
  end
  
  module Validations
    extend ActiveSupport::Concern
    include ActiveModel::Validations
    
    included do
      define_model_callbacks :validation
      define_callbacks :validate, :scope => :name
    end
    
    module ClassMethods
      def create!(attributes = {})
        new(attributes).tap do |object|
          object.save!
        end
      end
    end

    def valid?
      run_callbacks :validation do
        super
      end
    end

    def save(options={})
      perform_validations(options) ?  super : false
    end
    
    def save!
      save || raise(RecordInvalid.new(self))
    end

    protected
      def perform_validations(options={})
        (options[:validate] != false) ? valid? : true
      end
  end
end

require 'datastax_rails/validations/uniqueness'
