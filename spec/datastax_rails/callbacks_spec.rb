require 'spec_helper'

class CallbackTester < Hobby
  self.column_family = "hobbies"
  %w[before_save before_create after_save after_create before_validation after_validation
     after_touch after_initialize after_find before_destroy after_destroy].each do |callback|
       self.send(callback, callback + "_callback")   # after_save 'after_save_callback'
       define_method(callback+"_callback") do
         true
       end
     end
end

describe DatastaxRails::Base do
  describe "callbacks" do
    %w[before_save before_create after_save after_create before_validation after_validation].each do |callback|
      subject { CallbackTester.new(:name => callback) }
      
      it "runs #{callback}" do
        expect(subject).to receive(callback+"_callback")
        subject.save!
      end
    end
  end
end
