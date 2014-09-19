module DatastaxRails
  # = DatastaxRails Serialization
  module Serialization
    extend ActiveSupport::Concern
    include ActiveModel::Serializers::JSON

    def serializable_hash(options = nil)
      options = options.try(:clone) || {}

      options[:except] = Array.wrap(options[:except]).map { |n| n.to_s }

      hash = super(options)

      serializable_add_includes(options) do |association, records, opts|
        hash[association] = if records.is_a?(Enumerable)
                              records.map { |r| r.serializable_hash(opts) }
                            else
                              records.serializable_hash(opts)
                            end
      end

      serializable_convert_uuids(hash)

      hash
    end

    private

    def serializable_convert_uuids(hash)
      hash.each do |k, v|
        serializable_convert_uuids(v) if v.is_a?(Hash)
        hash[k] = v.to_s if v.is_a?(::Cql::Uuid)
      end
    end

    # Add associations specified via the <tt>:include</tt> option.
    #
    # Expects a block that takes as arguments:
    #   +association+ - name of the association
    #   +records+     - the association record(s) to be serialized
    #   +opts+        - options for the association records
    def serializable_add_includes(options = {})
      include_associations = options.delete(:include)
      return unless include_associations

      base_only_or_except = { except: options[:except],
                              only:   options[:only] }

      include_has_options = include_associations.is_a?(Hash)
      associations = include_has_options ? include_associations.keys : Array.wrap(include_associations)

      associations.each do |association|
        records = case self.class.reflect_on_association(association).macro
                  when :has_many, :has_and_belongs_to_many
                    send(association).to_a
                  when :has_one, :belongs_to
                    send(association)
                  end

        next unless records
        association_options = include_has_options ? include_associations[association] : base_only_or_except
        opts = options.merge(association_options)
        yield(association, records, opts)
      end

      options[:include] = include_associations
    end
  end
end

require 'datastax_rails/serializers/xml_serializer'
