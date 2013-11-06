module DatastaxRails
  module Callbacks
    extend ActiveSupport::Concern

    CALLBACKS = [
      :after_initialize, :after_find, :after_touch, :before_validation, :after_validation,
      :before_save, :around_save, :after_save, :before_create, :around_create,
      :after_create, :before_update, :around_update, :after_update,
      :before_destroy, :around_destroy, :after_destroy, :after_commit, :after_rollback
    ]


    included do
      extend ActiveModel::Callbacks
      include ActiveModel::Validations::Callbacks
      define_model_callbacks :initialize, :find, :touch, :only => :after
      define_model_callbacks :save, :create, :update, :destroy
    end

    def destroy(*) #:nodoc:
      #_run_destroy_callbacks { super }
      run_callbacks(:destroy) { super }
    end

    private
      def _create_or_update(*) #:nodoc:
        #_run_save_callbacks { super }
        run_callbacks(:save) { super }
      end

      def _create(*) #:nodoc:
        #_run_create_callbacks { super }
        run_callbacks(:create) { super }
      end

      def _update(*) #:nodoc:
        #_run_update_callbacks { super }
        run_callbacks(:update) { super }
      end
  end
end