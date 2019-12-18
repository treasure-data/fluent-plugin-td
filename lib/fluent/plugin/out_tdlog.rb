require 'fileutils'
require 'tempfile'
require 'zlib'
require 'stringio'
require 'td-client'

require 'fluent/plugin/output'
require 'fluent/plugin/td_plugin_version'

module Fluent::Plugin
  class TreasureDataLogOutput < Output
    Fluent::Plugin.register_output('tdlog', self)

    IMPORT_SIZE_LIMIT = 32 * 1024 * 1024
    UPLOAD_EXT = 'msgpack.gz'.freeze

    helpers :event_emitter, :compat_parameters

    config_param :apikey, :string, :secret => true
    config_param :auto_create_table, :bool, :default => true
    config_param :database, :string, :default => nil
    config_param :table, :string, :default => nil
    config_param :use_gzip_command, :bool, :default => false

    config_param :import_endpoint, :string, :alias => :endpoint, :default => TreasureData::API::DEFAULT_IMPORT_ENDPOINT
    config_param :api_endpoint, :string, :default => TreasureData::API::DEFAULT_ENDPOINT
    config_param :use_ssl, :bool, :default => true
    config_param :tmpdir, :string, :default => nil
    config_param :http_proxy, :string, :default => nil
    config_param :connect_timeout, :integer, :default => nil
    config_param :read_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil

    config_section :buffer do
      config_set_default :@type, 'file'
      config_set_default :chunk_keys, ['tag']
      config_set_default :flush_interval, 300
      config_set_default :chunk_limit_size, IMPORT_SIZE_LIMIT
    end

    def initialize
      super
      @key = nil
      @key_num_limit = 512  # TODO: Our one-time import has the restriction about the number of record keys.
      @record_size_limit = 32 * 1024 * 1024  # TODO
      @table_list = {}
      @empty_gz_data = TreasureData::API.create_empty_gz_data
      @user_agent = "fluent-plugin-td: #{TreasureDataPlugin::VERSION}".freeze
    end

    def configure(conf)
      compat_parameters_convert(conf, :buffer, default_chunk_key: 'tag')

      super

      if @use_gzip_command
        require 'open3'

        begin
          Open3.capture3("gzip -V")
        rescue Errno::ENOENT
          raise ConfigError, "'gzip' utility must be in PATH for use_gzip_command parameter"
        end
      end

      FileUtils.mkdir_p(@tmpdir) if @tmpdir

      if @database && @table
        validate_database_and_table_name(@database, @table)
        @key = "#{@database}.#{@table}"
      else
        unless @chunk_key_tag
          raise Fluent::ConfigError, "'tag' must be included in <buffer ARG> when database and table are not specified"
        end
      end
    end

    def start
      super

      client_opts = {
        :ssl => @use_ssl, :http_proxy => @http_proxy, :user_agent => @user_agent,
        :connect_timeout => @connect_timeout, :read_timeout => @read_timeout, :send_timeout => @send_timeout
      }
      @client = TreasureData::Client.new(@apikey, client_opts.merge({:endpoint => @import_endpoint}))
      @api_client = TreasureData::Client.new(@apikey, client_opts.merge({:endpoint => @api_endpoint}))
      if @key
        if @auto_create_table
          ensure_database_and_table(@database, @table)
        else
          check_table_exists(@key)
        end
      end
    end

    def multi_workers_ready?
      true
    end

    def formatted_to_msgpack_binary
      true
    end

    def format(tag, time, record)
      begin
        record['time'] = time.to_i
        record.delete(:time) if record.has_key?(:time)

        if record.size > @key_num_limit
          # TODO include summary of the record
          router.emit_error_event(tag, time, record, RuntimeError.new("too many number of keys (#{record.size} keys)"))
          return nil
        end
      rescue => e
        router.emit_error_event(tag, time, {'record' => record}, RuntimeError.new("skipped a broken record: #{e}"))
        return nil
      end

      begin
        result = record.to_msgpack
      rescue RangeError
        result = TreasureData::API.normalized_msgpack(record)
      rescue => e
        router.emit_error_event(tag, time, {'record' => record}, RuntimeError.new("can't convert record to msgpack: #{e}"))
        return nil
      end

      if result.bytesize > @record_size_limit
        # Don't raise error. Large size is not critical for streaming import
        log.warn "Size of a record too large (#{result.bytesize} bytes): #{summarize_record(record)}"
      end

      result
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

      if @key
        database, table = @database, @table
      else
        database, table = chunk.metadata.tag.split('.')[-2, 2]
        database = TreasureData::API.normalize_database_name(database)
        table = TreasureData::API.normalize_table_name(table)
      end

      FileUtils.mkdir_p(@tmpdir) unless @tmpdir.nil?
      f = Tempfile.new("tdlog-#{chunk.metadata.tag}-", @tmpdir)
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
      chunk_is_file = @buffer_config['@type'] == 'file'
      path = if chunk_is_file
               chunk.path
             else
               w = Tempfile.new("gzip-tdlog-#{chunk.metadata.tag}-", @tmpdir)
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
          @client.import(database, table, UPLOAD_EXT, io, size, unique_str)
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
          @client.import(database, table, UPLOAD_EXT, io, io.size)
          @table_list[key] = true
        rescue TreasureData::NotFoundError
          raise "Table #{key.inspect} does not exist on Treasure Data. Use 'td table:create #{database} #{table}' to create it."
        rescue => e
          log.warn "failed to check existence of '#{database}.#{table}' table on Treasure Data", :error => e.inspect
          log.debug_backtrace e.backtrace
        end
      end
    end

    def validate_database_and_table_name(database, table)
      begin
        TreasureData::API.validate_database_name(database)
      rescue => e
        raise ConfigError, "Invalid database name #{database.inspect}: #{e}"
      end
      begin
        TreasureData::API.validate_table_name(table)
      rescue => e
        raise ConfigError, "Invalid table name #{table.inspect}: #{e}"
      end
    end

    def ensure_database_and_table(database, table)
      log.info "Creating table #{database}.#{table} on TreasureData"
      begin
        @api_client.create_log_table(database, table)
      rescue TreasureData::NotFoundError
        @api_client.create_database(database)
        @api_client.create_log_table(database, table)
      rescue TreasureData::AlreadyExistsError
      end
    end
  end
end
