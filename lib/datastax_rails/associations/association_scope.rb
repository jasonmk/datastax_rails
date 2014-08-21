module DatastaxRails
  module Associations
    # Creates the scope (relation) for the assocation
    class AssociationScope
      attr_reader :association

      delegate :klass, :owner, :reflection, to: :association
      delegate :chain, :options, :datastax_rails, to: :reflection

      def initialize(association)
        @association = association
      end

      def scope
        scope = klass.unscoped
        scope = scope.extending(*Array.wrap(options[:extend]))

        if reflection.source_macro == :belongs_to
          scope.where('id' => owner.send(reflection.foreign_key))
        else
          scope.where(reflection.foreign_key => owner.id)
        end
      end
    end
  end
end
