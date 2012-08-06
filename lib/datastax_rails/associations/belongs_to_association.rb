module DatastaxRails
  module Associations
    class BelongsToAssociation < SingularAssociation #:nodoc:
      def replace(record)
        raise_on_type_mismatch(record) if record
        
        replace_keys(record)
        set_inverse_instance(record)
        
        @updated = true if record
        
        self.target = record
      end
      
      def updated?
        @updated
      end
      
      private
      
        def find_target?
          !loaded? && foreign_key_present? && klass
        end
        
        # Checks whether record is different to the current target, without loading it
        def different_target?(record)
          record.nil? && owner[reflection.foreign_key] ||
          record && record.id != owner[reflection.foreign_key]
        end
        
        def replace_keys(record)
          owner.loaded_attributes[reflection.foreign_key] = true
          owner.send("#{reflection.foreign_key}_will_change!")
          if record
            owner[reflection.foreign_key] = record.id
          else
            owner[reflection.foreign_key] = nil
          end
        end
        
        def foreign_key_present?
          owner[reflection.foreign_key]
        end
        
        # NOTE - for now, we're only supporting inverse setting from belongs_to back onto
        # has_one associations.
        def invertible_for?(record)
          inverse = inverse_reflection_for(record)
          inverse && inverse.macro == :has_one
        end

        def target_id
          if options[:primary_key]
            owner.send(reflection.name).try(:id)
          else
            owner[reflection.foreign_key]
          end
        end

        def stale_state
          owner[reflection.foreign_key].to_s
        end
    end
  end
end