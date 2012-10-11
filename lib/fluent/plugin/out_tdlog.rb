module Fluent


class TreasureDataLogOutput < BufferedOutput
  Plugin.register_output('tdlog', self)

  IMPORT_SIZE_LIMIT = 32*1024*1024

  class Anonymizer
    include Configurable
  end

  class RawAnonymizer < Anonymizer
    def anonymize(obj)
      if obj == nil
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

  def initialize
    require 'fileutils'
    require 'tempfile'
    require 'zlib'
    require 'net/http'
    require 'json'
    require 'cgi' # CGI.escape
    require 'time' # Time#rfc2822
    require 'td-client'
    require 'digest/md5'
    super
    @tmpdir = '/tmp/fluent/tdlog'
    @apikey = nil
    @key = nil
    @key_num_limit = 5120  # TODO
    @record_size_limit = 32*1024*1024  # TODO
    @table_list = []
    @auto_create_table = true
    @use_ssl = false
    @buffer_type = 'file'  # overwrite default buffer_type
    @flush_interval = 300  # overwrite default flush_interval to 5mins
  end

  def configure(conf)
    super

    # force overwrite buffer_chunk_limit
    if @buffer.respond_to?(:buffer_chunk_limit=) && @buffer.respond_to?(:buffer_queue_limit=)
      if @buffer.buffer_chunk_limit > IMPORT_SIZE_LIMIT
        ex = @buffer.buffer_chunk_limit / IMPORT_SIZE_LIMIT
        @buffer.buffer_chunk_limit = IMPORT_SIZE_LIMIT
        @buffer.buffer_queue_limit *= ex if ex > 0
      end
    end

    @tmpdir = conf['tmpdir'] || @tmpdir
    FileUtils.mkdir_p(@tmpdir)

    @apikey = conf['apikey']
    unless @apikey
      raise ConfigError, "'apikey' parameter is required on tdlog output"
    end

    if auto_create_table = conf['auto_create_table']
      if auto_create_table.empty?
        @auto_create_table = true
      else
        @auto_create_table = Config.bool_value(auto_create_table)
        if @auto_create_table == nil
          raise ConfigError, "'true' or 'false' is required for auto_create_table option on tdlog output"
        end
      end
    end

    if use_ssl = conf['use_ssl']
      if use_ssl.empty?
        @use_ssl = true
      else
        @use_ssl = Config.bool_value(use_ssl)
        if @use_ssl == nil
          raise ConfigError, "'true' or 'false' is required for use_ssl option on tdlog output"
        end
      end
    end

    unless @auto_create_table
      database = conf['database']
      table = conf['table']

      if !database || !table
        raise ConfigError, "'database' and 'table' parameter are required on tdlog output"
      end
      begin
        TreasureData::API.normalize_database_name(database)
      rescue
        raise ConfigError, "Invalid database name #{database.inspect}: #{$!}: #{conf}"
      end
      begin
        TreasureData::API.normalize_table_name(table)
      rescue
        raise ConfigError, "Invalid table name #{table.inspect}: #{$!}: #{conf}"
      end
      @key = "#{database}.#{table}"
    end

    @anonymizes = {}
    conf.elements.select {|e|
      e.name == 'anonymize'
    }.each {|e|
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
    @anonymizes = nil if @anonymizes.empty?

    @http_proxy = conf['http_proxy']
  end

  def start
    super
    @client = TreasureData::Client.new(@apikey, :ssl=>@use_ssl, :http_proxy=>@http_proxy)
    unless @auto_create_table
      check_table_exists(@key)
    end
  end

  def emit(tag, es, chain)
    if @key
      key = @key
    else
      database, table = tag.split('.')[-2,2]
      TreasureData::API.normalize_database_name(database)
      TreasureData::API.normalize_table_name(table)
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
    es.each {|time,record|
      begin
        if @anonymizes
          @anonymizes.each_pair {|key,scr|
            if value = record[key]
              record[key] = scr.anonymize(value)
            end
          }
        end

        record['time'] = time

        if record.size > @key_num_limit
          raise "Too many number of keys (#{record.size} keys)"  # TODO include summary of the record
        end

      rescue
        # TODO (a) Remove the transaction mechanism of fluentd
        #      or (b) keep transaction boundaries in in/out_forward.
        #      This code disables the transaction mechanism (a).
        $log.error "#{$!}: #{summarize_record(record)}"
        $log.error_backtrace $!.backtrace
        next
      end

      record.to_msgpack(out)

      noff = out.bytesize
      sz = noff - off
      if sz > @record_size_limit
        # TODO don't raise error
        #raise "Size of a record too large (#{sz} bytes)"  # TODO include summary of the record
        $log.warn "Size of a record too large (#{sz} bytes): #{summarize_record(record)}"
      end
      off = noff
    }
    out
  end

  def summarize_record(record)
    json = record.to_json
    if json.size > 100
      json[0..97]+"..."
    else
      json
    end
  end

  def write(chunk)
    unique_id = chunk.unique_id
    database, table = chunk.key.split('.',2)

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
    unique_str = unique_id.unpack('C*').map {|x| "%02x" % x }.join
    $log.trace { "uploading logs to Treasure Data database=#{database} table=#{table} (#{size}bytes)" }

    begin
      begin
        start = Time.now
        @client.import(database, table, "msgpack.gz", io, size, unique_str)
      rescue TreasureData::NotFoundError
        unless @auto_create_table
          raise $!
        end
        $log.info "Creating table #{database}.#{table} on TreasureData"
        begin
          @client.create_log_table(database, table)
        rescue TreasureData::NotFoundError
          @client.create_database(database)
          @client.create_log_table(database, table)
        end
        io.pos = 0
        retry
      end
    rescue => e
      elapsed = Time.now - start
      ne = RuntimeError.new("Failed to upload to TreasureData: #{$!} (#{size} bytes; #{elapsed} seconds)")
      ne.set_backtrace(e.backtrace)
      raise ne
    end
  end

  def check_table_exists(key)
    unless @table_list.include?(key)
      begin
        @table_list = get_table_list
      rescue
        $log.warn "failed to update table list on Treasure Data", :error=>$!.to_s
        $log.debug_backtrace $!
      end
      unless @table_list.include?(key)
        database, table = key.split('.',2)
        raise "Table #{key.inspect} does not exist on Treasure Data. Use 'td create-log-table #{database} #{table}' to create it."
      end
    end
  end

  def get_table_list
    $log.info "updating table list from Treasure Data"
    list = []
    @client.databases.each {|db|
      db.tables.each {|tbl|
        list << "#{db.name}.#{tbl.name}"
      }
    }
    list
  end
end


end
