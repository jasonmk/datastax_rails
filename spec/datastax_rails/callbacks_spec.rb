require 'spec_helper'

class CallbackTester < Hobby
  self.column_family = 'hobbies'
  attr_accessor :after_find_called, :after_initialize_called

  after_find { self.after_find_called = true }
  after_initialize { self.after_initialize_called = true }

  %w(before_save before_create after_save after_create before_validation after_validation
     after_touch before_destroy after_destroy).each do |callback|
       send(callback, callback + '_callback')   # after_save 'after_save_callback'
       define_method(callback + '_callback') do
         true
       end
     end
end

describe DatastaxRails::Base do
  describe 'callbacks' do
    subject { CallbackTester.new }

    %w(before_save before_create after_save after_create before_validation after_validation).each do |callback|
      it "runs #{callback}" do
        expect(subject).to receive(callback + '_callback')
        subject.save
      end
    end

    %w(after_destroy before_destroy).each do |callback|
      it "runs #{callback}" do
        subject.save
        expect(subject).to receive(callback + '_callback')
        subject.destroy
      end
    end

    it 'runs after_find' do
      subject.save
      ct = CallbackTester.find(subject.id)
      expect(ct.after_find_called).to be_truthy
    end

    it 'runs after_initialize' do
      expect(subject.after_initialize_called).to be_truthy
    end
  end
end
