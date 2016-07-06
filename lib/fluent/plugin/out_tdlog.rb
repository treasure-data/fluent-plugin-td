require 'td-client'
require 'fluent/output'
require 'fluent/plugin/td_plugin_version'

module Fluent
  class TreasureDataLogOutput < BufferedOutput
    Plugin.register_output('tdlog', self)

    IMPORT_SIZE_LIMIT = 32 * 1024 * 1024

    # To support log_level option since Fluentd v0.10.43
    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    config_param :apikey, :string, :secret => true
    config_param :auto_create_table, :bool, :default => true
    config_param :use_gzip_command, :bool, :default => false

    config_param :endpoint, :string, :default => TreasureData::API::NEW_DEFAULT_ENDPOINT
    config_param :use_ssl, :bool, :default => true
    config_param :connect_timeout, :integer, :default => nil
    config_param :read_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil
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
      # overwrite default value of buffer_chunk_limit
      unless conf.has_key?('buffer_chunk_limit')
        conf['buffer_chunk_limit'] = IMPORT_SIZE_LIMIT
      end

      # v0.14 seems to have a bug of config_set_default: https://github.com/treasure-data/fluent-plugin-td/pull/22#issuecomment-230782005
      unless conf.has_key?('buffer_type')
        conf['buffer_type'] = 'file'
      end

      super

      if @use_gzip_command
        require 'open3'

        begin
          Open3.capture3("gzip -V")
        rescue Errno::ENOENT
          raise ConfigError, "'gzip' utility must be in PATH for use_gzip_command parameter"
        end
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
      out = $use_msgpack_5 ? MessagePack::Buffer.new : ''.force_encoding('ASCII-8BIT') # this condition will be removed after removed msgpack v0.4 support
      off = out.size  # size is same as bytesize in ASCII-8BIT string
      es.each { |time, record|
        # Applications may send non-hash record or broken chunk may generate non-hash record so such records should be skipped
        next unless record.is_a?(Hash)

        begin
          record['time'] = time
          record.delete(:time) if record.has_key?(:time)

          if record.size > @key_num_limit
            raise "Too many number of keys (#{record.size} keys)"  # TODO include summary of the record
          end
        rescue => e
          # TODO (a) Remove the transaction mechanism of fluentd
          #      or (b) keep transaction boundaries in in/out_forward.
          #      This code disables the transaction mechanism (a).
          log.warn "Skipped a broken record (#{e}): #{summarize_record(record)}"
          log.warn_backtrace e.backtrace
          next
        end

        begin
          record.to_msgpack(out)
        rescue RangeError
          # In msgpack v0.5, 'out' becomes String, not Buffer. This is not a problem because Buffer has a compatibility with String
          out = out.to_s[0, off]
          TreasureData::API.normalized_msgpack(record, out)
        end

        noff = out.size
        sz = noff - off
        if sz > @record_size_limit
          # TODO don't raise error
          #raise "Size of a record too large (#{sz} bytes)"  # TODO include summary of the record
          log.warn "Size of a record too large (#{sz} bytes): #{summarize_record(record)}"
        end
        off = noff
      }
      out.to_s
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
      f = Tempfile.new("tdlog-#{chunk.key}-", @tmpdir)
      f.binmode

      size = if @use_gzip_command
               gzip_by_command(chunk, f)
             else
               gzip_by_writer(chunk, f)
             end
      f.pos = 0
      upload(database, table, f, size, unique_id)
    ensure
      f.close(true) if f
    end

    # TODO: Share this routine with s3 compressors
    def gzip_by_command(chunk, tmp)
      chunk_is_file = @buffer_type == 'file'
      path = if chunk_is_file
               chunk.path
             else
               w = Tempfile.new("gzip-tdlog-#{chunk.key}-", @tmpdir)
               w.binmode
               chunk.write_to(w)
               w.close
               w.path
             end
      res = system "gzip -c #{path} > #{tmp.path}"
      unless res
        log.warn "failed to execute gzip command. Fallback to GzipWriter. status = #{$?}"
        begin
          tmp.truncate(0)
          return gzip_by_writer(chunk, tmp)
        end
      end
      File.size(tmp.path)
    ensure
      unless chunk_is_file
        w.close(true) rescue nil
      end
    end

    def gzip_by_writer(chunk, tmp)
      w = Zlib::GzipWriter.new(tmp)
      chunk.write_to(w)
      w.finish
      w = nil
      tmp.pos
    ensure
      if w
        w.close rescue nil
      end
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
