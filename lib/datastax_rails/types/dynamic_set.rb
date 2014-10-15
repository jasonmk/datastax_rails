module DatastaxRails
  module Types
    # A collection type that allows you to store an un-ordered, unique
    # set of entries. Changes are tracked by hooking into ActiveModel's
    # built-in change tracking.
    class DynamicSet < Set
      include DirtyCollection

      delegate :join, :[], :to_xml, to: :to_a

      alias_method :to_ary, :to_a
    end
  end
end
