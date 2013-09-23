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


    # Runs all the validations within the specified context. Returns true if no errors are found,
    # false otherwise.
    #
    # If the argument is false (default is +nil+), the context is set to <tt>:create</tt> if
    # <tt>new_record?</tt> is true, and to <tt>:update</tt> if it is not.
    #
    # Validations with no <tt>:on</tt> option will run no matter the context. Validations with
    # some <tt>:on</tt> option will only run in the specified context.
    def valid?(context = nil)
      run_callbacks :validation do
        context ||= (new_record? ? :create : :update)
        output = super(context)
        errors.empty? && output
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
require 'datastax_rails/validations/associated'
