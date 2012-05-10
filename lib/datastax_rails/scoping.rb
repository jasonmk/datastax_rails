module DatastaxRails
  module Scoping
    extend ActiveSupport::Concern
    
    module ClassMethods
      def scoped(options = nil)
        if options
          scoped.apply_finder_options(options)
        else
          if current_scope
            current_scope.clone
          else
            relation.clone.tap do |scope|
              scope.default_scoped = true
            end
          end
        end
      end
      
      # Adds a class method for retrieving and querying objects. A \scope represents a narrowing of a SOLR query,
      # such as <tt>where(:color => :red).order(:size)</tt>.
      #
      #   class Shirt < DatastaxRails::Base
      #     scope :red, where(:color => 'red')
      #     scope :dry_clean_only, where(:dry_clean => true)
      #   end
      #
      # The above calls to <tt>scope</tt> define class methods Shirt.red and Shirt.dry_clean_only. Shirt.red,
      # in effect, represents the query <tt>Shirt.where(:color => 'red')</tt>.
      #
      # Note that this is simply 'syntactic sugar' for defining an actual class method:
      #
      #   class Shirt < ActiveRecord::Base
      #     def self.red
      #       where(:color => 'red')
      #     end
      #   end
      #
      # Unlike <tt>Shirt.find(...)</tt>, however, the object returned by Shirt.red is not an Array; it
      # resembles the association object constructed by a <tt>has_many</tt> declaration. For instance,
      # you can invoke <tt>Shirt.red.first</tt>, <tt>Shirt.red.count</tt>, <tt>Shirt.red.where(:size => 'small')</tt>.
      # Also, just as with the association objects, named \scopes act like an Array, implementing Enumerable;
      # <tt>Shirt.red.each(&block)</tt>, <tt>Shirt.red.first</tt>, and <tt>Shirt.red.inject(memo, &block)</tt>
      # all behave as if Shirt.red really was an Array.
      #
      # These named \scopes are composable. For instance, <tt>Shirt.red.dry_clean_only</tt> will produce
      # all shirts that are both red and dry clean only.
      # Nested finds and calculations also work with these compositions: <tt>Shirt.red.dry_clean_only.count</tt>
      # returns the number of garments for which these criteria obtain. Similarly with
      # <tt>Shirt.red.dry_clean_only.average(:thread_count)</tt>.
      #
      # All \scopes are available as class methods on the DatastaxRails::Base descendant upon which
      # the \scopes were defined. But they are also available to <tt>has_many</tt> associations. If,
      #
      #   class Person < DatastaxRails::Base
      #     has_many :shirts
      #   end
      #
      # then <tt>elton.shirts.red.dry_clean_only</tt> will return all of Elton's red, dry clean
      # only shirts.
      #
      # Named \scopes can also be procedural:
      #
      #   class Shirt < DatastaxRails::Base
      #     scope :colored, lambda { |color| where(:color => color) }
      #   end
      #
      # In this example, <tt>Shirt.colored('puce')</tt> finds all puce shirts.
      #
      # Note that scopes defined with \scope will be evaluated when they are defined, rather than
      # when they are used. For example, the following would be incorrect:
      #
      #   class Post < DatastaxRails::Base
      #     scope :recent, where('published_at >= ?', Time.current - 1.week)
      #   end
      #
      # The example above would be 'frozen' to the <tt>Time.current</tt> value when the <tt>Post</tt>
      # class was defined, and so the resultant SOLR query would always be the same. The correct
      # way to do this would be via a lambda, which will re-evaluate the scope each time
      # it is called:
      #
      #   class Post < DatastaxRails::Base
      #     scope :recent, lambda { where('published_at >= ?', Time.current - 1.week) }
      #   end
      #
      # Named \scopes can also have extensions, just as with <tt>has_many</tt> declarations:
      #
      #   class Shirt < DatastaxRails::Base
      #     scope :red, where(:color => 'red') do
      #       def dom_id
      #         'red_shirts'
      #       end
      #     end
      #   end
      #
      # Scopes can also be used while creating/building a record.
      #
      #   class Article < DatastaxRails::Base
      #     scope :published, where(:published => true)
      #   end
      #
      #   Article.published.new.published # => true
      #   Article.published.create.published # => true
      #
      # Class methods on your model are automatically available
      # on scopes. Assuming the following setup:
      #
      #   class Article < DatastaxRails::Base
      #     scope :published, where(:published => true)
      #     scope :featured, where(:featured => true)
      #
      #     def self.latest_article
      #       order('published_at desc').first
      #     end
      #
      #     def self.titles
      #       map(&:title)
      #     end
      #   end
      #
      # We are able to call the methods like this:
      #
      #   Article.published.featured.latest_article
      #   Article.featured.titles
      def scope(name, scope_options = {})
        name = name.to_sym
        valid_scope_name?(name)
        extension = Module.new(&Proc.new) if block_given?

        scope_proc = lambda do |*args|
          options = scope_options.respond_to?(:call) ? scope_options.call(*args) : scope_options
          options = scoped.apply_finder_options(options) if options.is_a?(Hash)

          relation = scoped.merge(options)

          extension ? relation.extending(extension) : relation
        end

        singleton_class.send(:redefine_method, name, &scope_proc)
      end
      
      # Returns a scope for this class without taking into account the default_scope.
        #
        #   class Post < DatastaxRails::Base
        #     def self.default_scope
        #       where :published => true
        #     end
        #   end
        #
        #   Post.all # Finds posts where +published+ is +true+
        #   Post.unscoped.all # Finds all posts regardless of +published+'s truthiness
        #
        # This method also accepts a block meaning that all queries inside the block will
        # not use the default_scope:
        #
        #   Post.unscoped {
        #     Post.limit(10) # Finds the first 10 posts
        #   }
        #
        # It is recommended to use block form of unscoped because chaining unscoped with <tt>scope</tt>
        # does not work. Assuming that <tt>published</tt> is a <tt>scope</tt> following two statements are same.
        #
        #   Post.unscoped.published
        #   Post.published
        def unscoped #:nodoc:
          block_given? ? relation.scoping { yield } : relation
        end
        
        def before_remove_const #:nodoc:
          self.current_scope = nil
        end
        
        # with_scope lets you apply options to inner block incrementally. It takes a hash and the keys must be
        # <tt>:find</tt> or <tt>:create</tt>. <tt>:find</tt> parameter is <tt>Relation</tt> while
        # <tt>:create</tt> parameters are an attributes hash.
        #
        #   class Article < DatastaxRails::Base
        #     def self.create_with_scope
        #       with_scope(:find => where(:blog_id => 1), :create => { :blog_id => 1 }) do
        #         find(1) # => WHERE blog_id = 1 AND id = 1
        #         a = create(1)
        #         a.blog_id # => 1
        #       end
        #     end
        #   end
        #
        # In nested scopings, all previous parameters are overwritten by the innermost rule, with the exception of
        # <tt>where</tt> which is merged.
        #
        # You can ignore any previous scopings by using the <tt>with_exclusive_scope</tt> method.
        #
        #   class Article < DatastaxRails::Base
        #     def self.find_with_exclusive_scope
        #       with_scope(:find => where(:blog_id => 1).limit(1)) do
        #         with_exclusive_scope(:find => limit(10)) do
        #           all # => SELECT * from articles LIMIT 10
        #         end
        #       end
        #     end
        #   end
        #
        # *Note*: the +:find+ scope also has effect on update and deletion methods, like +update_all+ and +delete_all+.
        def with_scope(scope = {}, action = :merge, &block)
          # If another DatastaxRails class has been passed in, get its current scope
          scope = scope.current_scope if !scope.is_a?(Relation) && scope.respond_to?(:current_scope)

          previous_scope = self.current_scope

          if scope.is_a?(Hash)
            # Dup first and second level of hash (method and params).
            scope = scope.dup
            scope.each do |method, params|
              scope[method] = params.dup unless params == true
            end

            scope.assert_valid_keys([ :find, :create ])
            relation = construct_finder_relation(scope[:find] || {})
            relation.default_scoped = true unless action == :overwrite

            if previous_scope && previous_scope.create_with_value && scope[:create]
              scope_for_create = if action == :merge
                previous_scope.create_with_value.merge(scope[:create])
              else
                scope[:create]
              end

              relation = relation.create_with(scope_for_create)
            else
              scope_for_create = scope[:create]
              scope_for_create ||= previous_scope.create_with_value if previous_scope
              relation = relation.create_with(scope_for_create) if scope_for_create
            end

            scope = relation
          end

          scope = previous_scope.merge(scope) if previous_scope && action == :merge

          self.current_scope = scope
          begin
            yield
          ensure
            self.current_scope = previous_scope
          end
        end
        
        # Works like with_scope, but discards any nested properties.
        def with_exclusive_scope(method_scoping = {}, &block)
          if method_scoping.values.any? { |e| e.is_a?(DatastaxRails::Relation) }
            raise ArgumentError, <<-MSG
New finder API can not be used with_exclusive_scope. You can either call unscoped to get an anonymous scope not bound to the default_scope:

User.unscoped.where(:active => true)

Or call unscoped with a block:

User.unscoped do
User.where(:active => true).all
end

MSG
          end
          with_scope(method_scoping, :overwrite, &block)
        end
        
        # Use this macro in your model to set a default scope for all operations on
        # the model.
        #
        #   class Article < DatastaxRails::Base
        #     default_scope where(:published => true)
        #   end
        #
        #   Article.all # => all articles where published = true
        #
        # The <tt>default_scope</tt> is also applied while creating/building a record. It is not
        # applied while updating a record.
        #
        #   Article.new.published # => true
        #   Article.create.published # => true
        #
        # You can also use <tt>default_scope</tt> with a block, in order to have it lazily evaluated:
        #
        #   class Article < DatastaxRails::Base
        #     default_scope { where(:published_at => Time.now - 1.week) }
        #   end
        #
        # (You can also pass any object which responds to <tt>call</tt> to the <tt>default_scope</tt>
        # macro, and it will be called when building the default scope.)
        #
        # If you use multiple <tt>default_scope</tt> declarations in your model then they will
        # be merged together:
        #
        #   class Article < DatastaxRails::Base
        #     default_scope where(:published => true)
        #     default_scope where(:rating => 'G')
        #   end
        #
        # Article.all # => all articles where published = true AND rating = 'G'
        #
        # This is also the case with inheritance and module includes where the parent or module
        # defines a <tt>default_scope</tt> and the child or including class defines a second one.
        #
        # If you need to do more complex things with a default scope, you can alternatively
        # define it as a class method:
        #
        #   class Article < DatastaxRails::Base
        #     def self.default_scope
        #       # Should return a scope, you can call 'super' here etc.
        #     end
        #   end
        def default_scope(scope = {})
          scope = Proc.new if block_given?
          self.default_scopes = default_scopes + [scope]
        end

        def build_default_scope #:nodoc:
          if method(:default_scope).owner != Base.singleton_class
            evaluate_default_scope { default_scope }
          elsif default_scopes.any?
            evaluate_default_scope do
              default_scopes.inject(relation) do |default_scope, scope|
                if scope.is_a?(Hash)
                  default_scope.apply_finder_options(scope)
                elsif !scope.is_a?(Relation) && scope.respond_to?(:call)
                  default_scope.merge(scope.call)
                else
                  default_scope.merge(scope)
                end
              end
            end
          end
        end

        def ignore_default_scope? #:nodoc:
          Thread.current["#{self}_ignore_default_scope"]
        end

        def ignore_default_scope=(ignore) #:nodoc:
          Thread.current["#{self}_ignore_default_scope"] = ignore
        end

        # The ignore_default_scope flag is used to prevent an infinite recursion situation where
        # a default scope references a scope which has a default scope which references a scope...
        def evaluate_default_scope
          return if ignore_default_scope?

          begin
            self.ignore_default_scope = true
            yield
          ensure
            self.ignore_default_scope = false
          end
        end
      
      # Collects attributes from scopes that should be applied when creating
      # an SO instance for the particular class this is called on.
      def scope_attributes # :nodoc:
        if current_scope
          current_scope.scope_for_create
        else
          relation.clone.tap do |scope|
            scope.default_scoped = true
          end
        end
      end

      # Are there default attributes associated with this scope?
      def scope_attributes? # :nodoc:
        current_scope || default_scopes.any?
      end
      
      protected
      
        def current_scope #:nodoc:
          Thread.current["#{self}_current_scope"]
        end
  
        def current_scope=(scope) #:nodoc:
          Thread.current["#{self}_current_scope"] = scope
        end
      
        def apply_default_scope
          
        end
      
        def valid_scope_name?(name)
          if respond_to?(name, true)
            logger.warn "Creating scope :#{name}. " \
                        "Overwriting existing method #{self.name}.#{name}."
          end
        end
    end
  end
end