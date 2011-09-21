module Fluent


class TreasureDataLogOutput < BufferedOutput
  Plugin.register_output('tdlog', self)

  HOST = ENV['TD_API_SERVER'] || 'api.treasure-data.com'
  PORT = 80
  USE_SSL = false

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
    @flush_interval = 300  # overwrite default flush_interval from 1min to 5mins
  end

  def configure(conf)
    super

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

    unless @auto_create_table
      database = conf['database']
      table = conf['table']

      if !database || !table
        raise ConfigError, "'database' and 'table' parameter are required on tdlog output"
      end
      if !validate_name(database)
        raise ConfigError, "Invalid database name #{database.inspect}: #{conf}"
      end
      if !validate_name(table)
        raise ConfigError, "Invalid table name #{table.inspect}: #{conf}"
      end
      @key = "#{database}.#{table}"
    end
  end

  def start
    super
    @client = TreasureData::Client.new(@apikey)
    unless @auto_create_table
      check_table_exists(@key)
    end
  end

  def emit(tag, es, chain)
    if @key
      key = @key
    else
      database, table = tag.split('.')[-2,2]
      if !validate_name(database) || !validate_name(table)
        $log.debug { "Invalid tag #{tag.inspect}" }
        return
      end
      key = "#{database}.#{table}"
    end

    unless @auto_create_table
      check_table_exists(key)
    end

    super(tag, es, chain, key)
  end

  def validate_name(name)
    true
  end

  def format_stream(tag, es)
    out = ''
    off = out.bytesize
    es.each {|event|
      record = event.record
      record['time'] = event.time

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
    if !validate_name(database) || !validate_name(table)
      $log.error "Invalid key name #{chunk.key.inspect}"
      return
    end

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
