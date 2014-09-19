require 'set'

# TODO: Break this up into manageable pieces
# rubocop:disable Style/RescueModifier
module DatastaxRails
  class Column # rubocop:disable Style/ClassLength
    TRUE_VALUES = [true, 1, '1', 't', 'T', 'true', 'TRUE', 'on', 'ON'].to_set
    FALSE_VALUES = [false, 0, '0', 'f', 'F', 'false', 'FALSE', 'off', 'OFF'].to_set

    module Format
      ISO_DATE = /\A(\d{4})-(\d\d)-(\d\d)\z/
      ISO_DATETIME = /\A(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(\.\d+)?\z/
      SOLR_TIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'.force_encoding('utf-8').freeze
    end

    attr_reader :name, :type, :cql_type, :solr_type, :options
    attr_accessor :primary, :coder, :default

    alias_method :encoded?, :coder

    # Instantiates a new column in the table.
    #
    # +name+ is the column's name as specified in the schema. e.g., 'first_name' in
    # <tt>first_name text</tt>.
    # +default+ is the type-casted default value that will be applied to a new record
    # if no value is given.
    # +type+ is the type of the column. Usually this will match the cql_type, but
    # there are exceptions (e.g., date)
    # +cql_type+ is the type of column as specified in the schema. e.g., 'text' in
    # <tt>first_name text</tt>.
    # +solr_type+ overrides the normal CQL <-> SOLR type mapping (uncommon)
    def initialize(name, default, type, options = {})
      @name      = name
      @type      = type.to_sym
      fail ArgumentError, "Unknown type #{type}" unless klass
      options[:holds] = 'string' if collection? && options[:holds].blank?
      @options   = configure_options(@type, options).with_indifferent_access
      @cql_type  = compute_cql_type(@type, @options)
      @solr_type = compute_solr_type(@type, @options)
      @default   = extract_default(default)
      @primary   = nil
      @coder     = nil
    end

    def configure_options(type, options)
      case type.to_sym
      when :set, :list, :map then
        configure_options(options[:holds], options).merge(multi_valued: true)
      when :binary then
        { solr_index: false,   solr_store: false,
          multi_valued: false, sortable: false,
          tokenized: false,    fulltext: false,
          cql_index: false }
      when :boolean, :date, :time, :timestamp, :datetime, :float, :integer, :uuid then
        { solr_index: true,    solr_store: true,
          multi_valued: false, sortable: true,
          tokenized: false,    fulltext: false,
          cql_index: false }
      when :string then
        { solr_index: true,    solr_store: true,
          multi_valued: false, sortable: true,
          tokenized: false,    fulltext: true,
          cql_index: false }
      when :text then
        { solr_index: true,    solr_store: true,
          multi_valued: false, sortable: false,
          tokenized: true,     fulltext: true,
          cql_index: false }
      else
        fail ArgumentError, "Unknown Type: #{type}"
      end.merge(options)
    end

    # Returns +true+ if the column is either of type ascii or text.
    def text?
      [:ascii, :text].include?(type)
    end

    # Returns +true+ if the column is either of type integer, float or decimal.
    def number?
      [:decimal, :double, :float, :integer].include?(type)
    end

    # Returns +true+ if the column is of type binary
    def binary?
      [:binary].include?(type)
    end

    def collection?
      [:set, :list, :map].include?(type)
    end

    def default?
      default.present?
    end

    # Returns the Ruby class that corresponds to the abstract data type.
    def klass
      case type
      when :integer                        then Fixnum
      when :float                          then Float
      when :decimal, :double               then BigDecimal
      when :timestamp, :time, :datetime    then Time
      when :date                           then Date
      when :text, :string, :binary, :ascii then String
      when :boolean                        then Object
      when :uuid                           then ::Cql::TimeUuid
      when :list                           then DatastaxRails::Types::DynamicList
      when :set                            then DatastaxRails::Types::DynamicSet
      when :map                            then DatastaxRails::Types::DynamicMap
      end
    end

    # Casts value (which can be a String) to an appropriate instance.
    def type_cast(value, record = nil, dest_type = nil) # rubocop:disable Style/CyclomaticComplexity
      return nil if value.nil?
      return coder.load(value) if encoded?

      klass = self.class

      case dest_type || type
      when :string, :text        then value.to_s
      when :ascii                then value.force_encoding('ascii')
      when :integer              then klass.value_to_integer(value)
      when :float                then value.to_f
      when :decimal              then klass.value_to_decimal(value)
      when :datetime, :timestamp then klass.string_to_time(value)
      when :time                 then klass.string_to_dummy_time(value)
      when :date                 then klass.value_to_date(value)
      when :binary               then klass.binary_to_string(value)
      when :boolean              then klass.value_to_boolean(value)
      when :uuid, :timeuuid      then klass.value_to_uuid(value)
      when :list, :set           then wrap_collection(value.map { |v| type_cast(v, record, @options[:holds]) }, record)
      when :map
        wrap_collection(value.each { |k, v| value[k] = type_cast(v, record, @options[:holds]) }.stringify_keys, record)
      else value
      end
    end

    def wrap_collection(collection, record)
      Types::DirtyCollection.ignore_modifications do
        klass.new(record, name, collection)
      end
    end

    # Cql-rb does a really good job of typecasting, so for the most part we
    # just pass in the native types.  The only exceptions are for UUIDs that
    # are passed in as strings and dates.
    def type_cast_for_cql3(value, dest_type = nil)
      return nil if value.nil?
      return coder.dump(value) if encoded?

      case (dest_type || type)
      when :uuid                        then value.is_a?(::Cql::Uuid) ? value : self.class.value_to_uuid(value)
      when :time, :datetime, :timestamp then value.to_time.utc
      when :date                        then value.to_time.utc
      when :list, :set                  then list_to_cql3_value(value)
      when :map                         then map_to_cql3_value(value)
      else value
      end
    end

    # By contrast, since Solr isn't doing things like prepared statements
    # it doesn't know what the types are so we have to handle any casting
    # or encoding ourselves.
    def type_cast_for_solr(value, dest_type = nil)
      return nil if value.nil?
      return coder.dump(value) if encoded?

      case (dest_type || type)
      when :boolean                            then value ? 'true' : 'false'
      when :date, :time, :datetime, :timestamp then value.to_time.utc.strftime(Format::SOLR_TIME_FORMAT)
      when :list, :set                         then list_to_solr_value(value)
      when :map                                then map_to_solr_value(value)
      when :uuid                               then value.to_s
      else value
      end
    end

    def list_to_solr_value(value)
      value.map { |v| type_cast_for_solr(v, @options[:holds].to_sym) }
    end

    def map_to_solr_value(value)
      value.each { |k, v| value[k] = type_cast_for_solr(v, @options[:holds].to_sym) }
    end

    def list_to_cql3_value(value)
      value.map { |v| type_cast_for_cql3(v, @options[:holds].to_sym) }
    end

    def map_to_cql3_value(value)
      value.dup.each { |k, v| value[k] = type_cast_for_cql3(v, @options[:holds].to_sym) }
      value
    end

    # Returns the human name of the column name.
    #
    # ===== Examples
    #  Column.new('sales_stage', ...).human_name # => 'Sales stage'
    def human_name
      Base.human_attribute_name(@name)
    end

    def extract_default(default)
      case type
      when :map      then {} # lambda {|rec| DatastaxRails::Types::DynamicMap.new(rec, self.name.to_s, {})}
      when :list     then [] # lambda {|rec| DatastaxRails::Types::DynamicList.new(rec, self.name.to_s, [])}
      when :set      then Set.new # lambda {|set| DatastaxRails::Types::DynamicSet.new(rec, self.name.to_s, Set.new)}
      else default
      end
    end

    # Used to convert from Strings to BLOBs
    def string_to_binary(value)
      self.class.string_to_binary(value)
    end

    def full_solr_range
      if %w(date uuid integer int double long float).include? solr_type
        '[* TO *]'
      else
        '[\"\" TO *]'
      end
    end

    class << self
      # Used to convert from Strings to BLOBs
      def string_to_binary(value)
        # TODO: Figure out what Cassandra's blobs look like
        value
      end

      # Used to convert from BLOBs to Strings
      def binary_to_string(value)
        # TODO: Figure out what Cassandra's blobs look like
        value
      end

      def value_to_date(value)
        if value.is_a?(String)
          return nil if value.empty?
          fast_string_to_date(value) || fallback_string_to_date(value)
        elsif value.respond_to?(:to_date)
          value.to_date
        else
          value
        end
      end

      def string_to_time(string)
        return string unless string.is_a?(String)
        return nil if string.empty?

        fast_string_to_time(string) || fallback_string_to_time(string)
      end

      def string_to_dummy_time(string)
        return string unless string.is_a?(String)
        return nil if string.empty?

        dummy_time_string = "2000-01-01 #{string}"

        fast_string_to_time(dummy_time_string) || begin
          time_hash = Date._parse(dummy_time_string)
          return nil if time_hash[:hour].nil?
          new_time(*time_hash.values_at(:year, :mon, :mday, :hour, :min, :sec, :sec_fraction))
        end
      end

      # convert something to a boolean
      def value_to_boolean(value)
        if value.is_a?(String) && value.empty?
          nil
        else
          TRUE_VALUES.include?(value)
        end
      end

      # Used to convert values to integer.
      def value_to_integer(value)
        case value
        when TrueClass, FalseClass
          value ? 1 : 0
        else
          value.to_i rescue nil
        end
      end

      # convert something to a BigDecimal
      def value_to_decimal(value)
        # Using .class is faster than .is_a? and
        # subclasses of BigDecimal will be handled
        # in the else clause
        if value.class == BigDecimal
          value
        elsif value.respond_to?(:to_d)
          value.to_d
        else
          value.to_s.to_d
        end
      end

      # convert something to a TimeUuid
      def value_to_uuid(value)
        if value.is_a?(::Cql::Uuid)
          value
        else
          ::Cql::TimeUuid.new(value) rescue nil
        end
      end

      protected

      # '0.123456' -> 123456
      # '1.123456' -> 123456
      def microseconds(time)
        time[:sec_fraction] ? (time[:sec_fraction] * 1_000_000).to_i : 0
      end

      def new_date(year, mon, mday)
        year && year != 0 && Date.new(year, mon, mday) rescue nil
      end

      def new_time(year, mon, mday, hour, min, sec, microsec, offset = nil) # rubocop:disable Style/ParameterLists
        # Treat 0000-00-00 00:00:00 as nil.
        return nil if year.nil? || (year == 0 && mon == 0 && mday == 0)

        if offset
          time = Time.utc(year, mon, mday, hour, min, sec, microsec) rescue nil
          return nil unless time

          time -= offset
          Base.default_timezone == :utc ? time : time.getlocal
        else
          Time.public_send(Base.default_timezone, year, mon, mday, hour, min, sec, microsec) rescue nil
        end
      end

      def fast_string_to_date(string)
        if string =~ Format::ISO_DATE
          new_date Regexp.last_match[1].to_i, Regexp.last_match[2].to_i, Regexp.last_match[3].to_i
        end
      end

      # Doesn't handle time zones.
      def fast_string_to_time(string)
        return unless string =~ Format::ISO_DATETIME
        microsec = (Regexp.last_match[7].to_r * 1_000_000).to_i
        new_time(Regexp.last_match[1].to_i,
                 Regexp.last_match[2].to_i,
                 Regexp.last_match[3].to_i,
                 Regexp.last_match[4].to_i,
                 Regexp.last_match[5].to_i,
                 Regexp.last_match[6].to_i,
                 microsec
                )
      end

      def fallback_string_to_date(string)
        new_date(*::Date._parse(string, false).values_at(:year, :mon, :mday))
      end

      def fallback_string_to_time(string)
        time_hash = Date._parse(string)
        time_hash[:sec_fraction] = microseconds(time_hash)

        new_time(*time_hash.values_at(:year, :mon, :mday, :hour, :min, :sec, :sec_fraction))
      end
    end

    private

    def compute_cql_type(field_type, options)
      options[:cql_type] ||
      case field_type.to_sym
      when :integer                            then 'int'
      when :time, :date, :timestamp, :datetime then 'timestamp'
      when :binary                             then 'blob'
      when :list                               then "list<#{compute_cql_type(options[:holds], options)}>"
      when :set                                then "set<#{compute_cql_type(options[:holds], options)}>"
      when :map                                then "map<text, #{compute_cql_type(options[:holds], options)}>"
      when :string                             then 'text'
      else field_type.to_s
      end
    end

    def compute_solr_type(field_type, options)
      options[:solr_type] ||
      case field_type.to_sym
      when :integer                            then 'int'
      when :decimal                            then 'double'
      when :timestamp, :time, :datetime        then 'date'
      when :list, :set, :map                   then compute_solr_type(options[:holds], options)
      else field_type.to_s
      end
    end
  end
end
