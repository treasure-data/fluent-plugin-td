module Fluent


class TreasureDataLogOutput < BufferedOutput
  Plugin.register_output('tdlog', self)

  IMPORT_SIZE_LIMIT = 32*1024*1024

  def initialize
    require 'fileutils'
    require 'tempfile'
    require 'zlib'
    require 'net/http'
    require 'json'
    require 'cgi' # CGI.escape
    require 'time' # Time#rfc2822
    require 'td-client'
    super
    @tmpdir = '/tmp/fluent/tdlog'
    @apikey = nil
    @key = nil
    @key_num_limit = 5120  # TODO
    @record_size_limit = 32*1024*1024  # TODO
    @table_list = []
    @auto_create_table = true
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
        TreasureData::API.validate_database_name(database)
      rescue
        raise ConfigError, "Invalid database name #{database.inspect}: #{$!}: #{conf}"
      end
      begin
        TreasureData::API.validate_table_name(table)
      rescue
        raise ConfigError, "Invalid table name #{table.inspect}: #{$!}: #{conf}"
      end
      @key = "#{database}.#{table}"
    end
  end

  def start
    super
    @client = TreasureData::Client.new(@apikey, :ssl=>@use_ssl)
    unless @auto_create_table
      check_table_exists(@key)
    end
  end

  def emit(tag, es, chain)
    if @key
      key = @key
    else
      database, table = tag.split('.')[-2,2]
      TreasureData::API.validate_database_name(database)
      TreasureData::API.validate_table_name(table)
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
      record['time'] = time

      if record.size > @key_num_limit
        raise "Too many number of keys (#{record.size} keys)"  # TODO include summary of the record
      end

      record.to_msgpack(out)

      noff = out.bytesize
      sz = noff - off
      if sz > @record_size_limit
        raise "Size of a record too large (#{sz} bytes)"  # TODO include summary of the record
      end
      off = noff
    }
    out
  end

  def write(chunk)
    database, table = chunk.key.split('.',2)

    f = Tempfile.new("tdlog-", @tmpdir)
    w = Zlib::GzipWriter.new(f)

    chunk.write_to(w)
    w.finish
    w = nil

    size = f.pos
    f.pos = 0
    upload(database, table, f, size)

  ensure
    w.close if w
    f.close if f
  end

  def upload(database, table, io, size)
    $log.trace { "uploading logs to Treasure Data database=#{database} table=#{table} (#{size}bytes)" }

    begin
      begin
        start = Time.now
        @client.import(database, table, "msgpack.gz", io, size)
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
