require 'set'

module DatastaxRails
  class Column
    TRUE_VALUES = [true, 1, '1', 't', 'T', 'true', 'TRUE', 'on', 'ON'].to_set
    FALSE_VALUES = [false, 0, '0', 'f', 'F', 'false', 'FALSE', 'off', 'OFF'].to_set
    
    module Format
      ISO_DATE = /\A(\d{4})-(\d\d)-(\d\d)\z/
      ISO_DATETIME = /\A(\d{4})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(\.\d+)?\z/
      SOLR_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ".force_encoding('utf-8').freeze
    end

    attr_reader :name, :default, :type, :cql_type, :solr_type, :options
    attr_accessor :primary, :coder

    alias :encoded? :coder

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
      raise ArgumentError, "Unknown type #{type}" unless self.klass
      @cql_type  = compute_cql_type(type, options)
      @solr_type = compute_solr_type(type, options)
      @default   = extract_default(default)
      @options   = configure_options(type, options).with_indifferent_access
      @primary   = nil
      @coder     = nil
    end
    
    def configure_options(type, options)
      case type
      when :set, :list, :map then
        configure_options(options[:type], options).merge(:multi_valued => true)
      when :binary then
        {:solr_index => false,   :solr_store => false, 
         :multi_valued => false, :sortable => false, 
         :tokenized => false,    :fulltext => false,
         :cql_index => false}
      when :boolean, :date, :time, :timestamp, :datetime, :float, :integer, :uuid then
        {:solr_index => true,    :solr_store => true,
         :multi_valued => false, :sortable => true,
         :tokenized => false,    :fulltext => false,
         :cql_index => false}
      when :string then
        {:solr_index => true,    :solr_store => true,
         :multi_valued => false, :sortable => true,
         :tokenized => false,    :fulltext => true,
         :cql_index => false}
      when :text then
        {:solr_index => true,    :solr_store => true,
         :multi_valued => false, :sortable => false,
         :tokenized => true,     :fulltext => true,
         :cql_index => false}
      else
        raise ArgumentError, "Unknown Type: #{type.to_s}"
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

    def has_default?
      !default.nil?
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
      when :list, :set                     then Array
      when :map                            then Hash
      end
    end

    # Casts value (which can be a String) to an appropriate instance.
    def type_cast(value, dest_type = nil)
      return nil if value.nil?
      return coder.load(value) if encoded?
      dest_type ||= type

      klass = self.class

      case dest_type
      when :string, :text        then value
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
      when :list, :set           then value.collect {|v| type_cast(v,@options[:type])}
      when :map                  then value.collect {|a,b| {a => type_cast(b,@options[:type])}}
      else value
      end
    end
    
    # Cql-rb does a really good job of typecasting, so for the most part we
    # just pass in the native types.  The only exceptions are for UUIDs that
    # are passed in as strings and dates.
    def type_cast_for_cql3(value)
      return nil if value.nil?
      return coder.dump(value) if encoded?
      
      if type == :uuid && value.class == String
        self.class.value_to_uuid(value)
      elsif type == :date
        value.to_time
      else
        value
      end
    end
    
    # By contrast, since Solr isn't doing things like prepared statements
    # it doesn't know what the types are so we have to handle any casting
    # or encoding ourselves.
    def type_cast_for_solr(value, column_type = nil)
      return nil if value.nil?
      return coder.dump(value) if encoded?
      
      case (column_type || type)
      when :boolean                            then value ? 1 : 0
      when :date, :time, :datetime, :timestamp then value.strftime(Format::SOLR_TIME_FORMAT)
      when :list, :set                         then self.list_to_solr_value(value)
      when :map                                then self.map_to_solr_value(value)
      else value
      end
    end
    
    def list_to_solr_value(value)
      value.map {|v| type_cast_for_solr(v, @options[:type])}
    end
    
    def map_to_solr_value(value)
      value.map { |a,b| { a.to_s => type_cast_for_solr(b, @options[:type]) } }
    end

    # Returns the human name of the column name.
    #
    # ===== Examples
    #  Column.new('sales_stage', ...).human_name # => 'Sales stage'
    def human_name
      Base.human_attribute_name(@name)
    end

    def extract_default(default)
      type_cast(default)
    end

    # Used to convert from Strings to BLOBs
    def string_to_binary(value)
      self.class.string_to_binary(value)
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
          if year && year != 0
            Date.new(year, mon, mday) rescue nil
          end
        end

        def new_time(year, mon, mday, hour, min, sec, microsec, offset = nil)
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
            new_date $1.to_i, $2.to_i, $3.to_i
          end
        end

        # Doesn't handle time zones.
        def fast_string_to_time(string)
          if string =~ Format::ISO_DATETIME
            microsec = ($7.to_r * 1_000_000).to_i
            new_time $1.to_i, $2.to_i, $3.to_i, $4.to_i, $5.to_i, $6.to_i, microsec
          end
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
        options[:cql_type] || case type.to_sym
        when :integer                        then 'int'
        when :time, :date                    then 'timestamp' 
        when :binary                         then 'blob'
        when :list                           then "list<#{options[:type] || text}>"
        when :set                            then "set<#{options[:type] || text}>"
        when :map                            then "map<text, #{options[:type] || text}>"
        when :string                         then 'text'
        else field_type.to_s
        end
      end
      
      def compute_solr_type(field_type, options)
        options[:solr_type] || case type.to_sym
        when :integer                        then 'int'
        when :decimal                        then 'double'
        when :timestamp, :time               then 'date'
        when :list, :set, :map               then options[:type].to_s || 'string'
        else field_type.to_s
        end
      end
  end
end
