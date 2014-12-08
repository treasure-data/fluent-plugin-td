require 'td-client'
require 'fluent/plugin/td_plugin_version'

module Fluent
  class TreasureDataLogOutput < BufferedOutput
    Plugin.register_output('tdlog', self)

    IMPORT_SIZE_LIMIT = 32 * 1024 * 1024

    class Anonymizer
      include Configurable
    end

    class RawAnonymizer < Anonymizer
      def anonymize(obj)
        if obj.nil?
          nil
        elsif obj.is_a?(String)
          anonymize_raw obj
        elsif obj.is_a?(Numeric)
          anonymize_raw obj.to_s
        else
          # boolean, array, map
          anonymize_raw MessagePack.pack(obj)
        end
      end
    end

    class MD5Anonymizer < RawAnonymizer
      def anonymize_raw(raw)
        Digest::MD5.hexdigest(raw)
      end
    end

    class IPXORAnonymizer < RawAnonymizer
      config_param :xor_key, :string

      def configure(conf)
        super

        a1, a2, a3, a4 = @xor_key.split('.')
        @xor_keys = [a1.to_i, a2.to_i, a3.to_i, a4.to_i]

        if @xor_keys == [0, 0, 0, 0]
          raise ConfigError, "'xor_key' must be IPv4 address"
        end
      end

      def anonymize_raw(raw)
        m = /\A(\d+)\.(\d+)\.(\d+)\.(\d+)/.match(raw)
        return nil unless m

        k1, k2, k3, k4 = @xor_keys

        o1 = m[1].to_i ^ k1
        o2 = m[2].to_i ^ k2
        o3 = m[3].to_i ^ k3
        o4 = m[4].to_i ^ k4

        "#{o1}.#{o2}.#{o3}.#{o4}"
      end
    end

    # To support log_level option since Fluentd v0.10.43
    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    config_param :apikey, :string
    config_param :auto_create_table, :bool, :default => true

    config_param :endpoint, :string, :default => TreasureData::API::NEW_DEFAULT_ENDPOINT
    config_param :use_ssl, :bool, :default => true
    config_param :connect_timeout, :integer, :default => nil
    config_param :read_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil
    config_set_default :buffer_type, 'file'
    config_set_default :flush_interval, 300

    def initialize
      require 'fileutils'
      require 'tempfile'
      require 'zlib'
      require 'net/http'
      require 'json'
      require 'cgi' # CGI.escape
      require 'time' # Time#rfc2822
      require 'digest/md5'
      require 'stringio'
      super
      @tmpdir = nil
      @key = nil
      @key_num_limit = 512  # TODO: Our one-time import has the restriction about the number of record keys.
      @record_size_limit = 32 * 1024 * 1024  # TODO
      @table_list = {}
      @empty_gz_data = TreasureData::API.create_empty_gz_data
      @user_agent = "fluent-plugin-td: #{TreasureDataPlugin::VERSION}".freeze
    end

    def configure(conf)
      super

      # overwrite default value of buffer_chunk_limit
      if !conf['buffer_chunk_limit']
        @buffer.buffer_chunk_limit = IMPORT_SIZE_LIMIT
      end

      if conf.has_key?('tmpdir')
        @tmpdir = conf['tmpdir']
        FileUtils.mkdir_p(@tmpdir)
      end

      database = conf['database']
      table = conf['table']
      if database && table
        validate_database_and_table_name(database, table, conf)
        @key = "#{database}.#{table}"
      end

      @anonymizes = {}
      conf.elements.select { |e|
        e.name == 'anonymize'
      }.each { |e|
        key = e['key']
        method = e['method']

        case method
        when 'md5'
          scr = MD5Anonymizer.new
        when 'ip_xor'
          scr = IPXORAnonymizer.new
        else
          raise ConfigError, "Unknown anonymize method: #{method}"
        end

        scr.configure(e)

        @anonymizes[key] = scr
      }
      if @anonymizes.empty?
        @anonymizes = nil
      else
        log.warn "<anonymize> feature is deprecated and will be removed. Use fluent-plugin-anonymizer instead."
      end

      @http_proxy = conf['http_proxy']
    end

    def start
      super

      client_opts = {
        :ssl => @use_ssl, :http_proxy => @http_proxy, :user_agent => @user_agent, :endpoint => @endpoint,
        :connect_timeout => @connect_timeout, :read_timeout => @read_timeout, :send_timeout => @send_timeout
      }
      @client = TreasureData::Client.new(@apikey, client_opts)

      if @key
        if @auto_create_table
          database, table = @key.split('.',2)
          ensure_database_and_table(database, table)
        else
          check_table_exists(@key)
        end
      end
    end

    def emit(tag, es, chain)
      if @key
        key = @key
      else
        database, table = tag.split('.')[-2,2]
        database = TreasureData::API.normalize_database_name(database)
        table = TreasureData::API.normalize_table_name(table)
        key = "#{database}.#{table}"
      end

      unless @auto_create_table
        check_table_exists(key)
      end

      super(tag, es, chain, key)
    end

    def format_stream(tag, es)
      out = ''
      off = out.bytesize
      es.each { |time, record|
        begin
          if @anonymizes
            @anonymizes.each_pair { |key, scr|
              if value = record[key]
                record[key] = scr.anonymize(value)
              end
            }
          end

          record['time'] = time
          record.delete(:time) if record.has_key?(:time)

          if record.size > @key_num_limit
            raise "Too many number of keys (#{record.size} keys)"  # TODO include summary of the record
          end
        rescue => e
          # TODO (a) Remove the transaction mechanism of fluentd
          #      or (b) keep transaction boundaries in in/out_forward.
          #      This code disables the transaction mechanism (a).
          log.error "#{e}: #{summarize_record(record)}"
          log.error_backtrace e.backtrace
          next
        end

        begin
          record.to_msgpack(out)
        rescue RangeError
          TreasureData::API.normalized_msgpack(record, out)
        end

        noff = out.bytesize
        sz = noff - off
        if sz > @record_size_limit
          # TODO don't raise error
          #raise "Size of a record too large (#{sz} bytes)"  # TODO include summary of the record
          log.warn "Size of a record too large (#{sz} bytes): #{summarize_record(record)}"
        end
        off = noff
      }
      out
    end

    def summarize_record(record)
      json = Yajl.dump(record)
      if json.size > 100
        json[0..97] + "..."
      else
        json
      end
    end

    def write(chunk)
      unique_id = chunk.unique_id
      database, table = chunk.key.split('.', 2)

      FileUtils.mkdir_p(@tmpdir) unless @tmpdir.nil?
      f = Tempfile.new("tdlog-", @tmpdir)
      w = Zlib::GzipWriter.new(f)

      chunk.write_to(w)
      w.finish
      w = nil

      size = f.pos
      f.pos = 0
      upload(database, table, f, size, unique_id)

    ensure
      w.close if w
      f.close if f
    end

    def upload(database, table, io, size, unique_id)
      unique_str = unique_id.unpack('C*').map { |x| "%02x" % x }.join
      log.trace { "uploading logs to Treasure Data database=#{database} table=#{table} (#{size}bytes)" }

      begin
        begin
          start = Time.now
          @client.import(database, table, "msgpack.gz", io, size, unique_str)
        rescue TreasureData::NotFoundError => e
          unless @auto_create_table
            raise e
          end
          ensure_database_and_table(database, table)
          io.pos = 0
          retry
        end
      rescue => e
        elapsed = Time.now - start
        ne = RuntimeError.new("Failed to upload to Treasure Data '#{database}.#{table}' table: #{e.inspect} (#{size} bytes; #{elapsed} seconds)")
        ne.set_backtrace(e.backtrace)
        raise ne
      end
    end

    def check_table_exists(key)
      unless @table_list.has_key?(key)
        database, table = key.split('.', 2)
        log.debug "checking whether table '#{database}.#{table}' exists on Treasure Data"
        io = StringIO.new(@empty_gz_data)
        begin
          @client.import(database, table, "msgpack.gz", io, io.size)
          @table_list[key] = true
        rescue TreasureData::NotFoundError
          raise "Table #{key.inspect} does not exist on Treasure Data. Use 'td table:create #{database} #{table}' to create it."
        rescue => e
          log.warn "failed to check existence of '#{database}.#{table}' table on Treasure Data", :error => e.inspect
          log.debug_backtrace e.backtrace
        end
      end
    end

    def validate_database_and_table_name(database, table, conf)
      begin
        TreasureData::API.validate_database_name(database)
      rescue => e
        raise ConfigError, "Invalid database name #{database.inspect}: #{e}: #{conf}"
      end
      begin
        TreasureData::API.validate_table_name(table)
      rescue => e
        raise ConfigError, "Invalid table name #{table.inspect}: #{e}: #{conf}"
      end
    end

    def ensure_database_and_table(database, table)
      log.info "Creating table #{database}.#{table} on TreasureData"
      begin
        @client.create_log_table(database, table)
      rescue TreasureData::NotFoundError
        @client.create_database(database)
        @client.create_log_table(database, table)
      rescue TreasureData::AlreadyExistsError
      end
    end
  end
end
