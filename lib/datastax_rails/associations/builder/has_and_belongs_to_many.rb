module DatastaxRails::Associations::Builder #:nodoc:
  class HasAndBelongsToMany < CollectionAssociation #:nodoc:
    self.macro = :has_and_belongs_to_many
    
    def build
      reflection = super
      define_destroy_hook
      check_for_join_column_family
      reflection
    end
    
    private
      def define_destroy_hook
        name = self.name
        model.send(:include, Module.new {
          class_eval <<-RUBY, __FILE__, __LINE__ + 1
            def destroy_associations
              association(#{name.to_sym.inspect}).delete_all_on_destroy
              super
            end
          RUBY
        })
      end
      
      def check_for_join_column_family
        unless DatastaxRails::Base.connection.column_families.has_key?("many_to_many_joins")
          cf = Cassandra::ColumnFamily.new
          cf.name = "many_to_many_joins"
          cf.keyspace = DatastaxRails::Base.connection.keyspace
          cf.comparator_type = 'BytesType'
          cf.column_type = 'Standard'
          DatastaxRails::Base.connection.add_column_family(cf)
        end
      end
  end
end