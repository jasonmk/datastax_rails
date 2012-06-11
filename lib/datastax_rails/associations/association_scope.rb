module DatastaxRails
  module Associations
    class AssociationScope #:nodoc:
      attr_reader :association
      
      delegate :klass, :owner, :reflection, :to => :association
      delegate :chain, :options, :datastax_rails, :to => :reflection
      
      def initialize(association)
        @association = association
      end
      
      def scope
        scope = klass.unscoped
        scope = scope.extending(*Array.wrap(options[:extend]))
        
        scope.where(reflection.foreign_key => owner.id)
      end
    end
  end
end