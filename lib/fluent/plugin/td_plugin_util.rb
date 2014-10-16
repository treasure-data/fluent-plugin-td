module Fluent
  module TDPluginUtil
    require 'fileutils'
    require 'stringio'
    require 'tempfile'
    require 'zlib'
    require 'td-client'

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

    def parse_bool_parameter(param)
      if param.empty?
        true
      else
        param = Config.bool_value(param)
        raise ConfigError, "'true' or 'false' is required for #{key} option on tdlog output" if param.nil?
        param
      end
    end

    def summarize_record(record)
      json = record.to_json
      if json.size > 100
        json[0..97] + "..."
      else
        json
      end
    end

    def check_table_existence(database, table)
      @table_list ||= {}
      key = "#{database}.#{table}"
      unless @table_list.has_key?(key)
        log.debug "checking whether table '#{key}' exists on Treasure Data"
        io = StringIO.new(@empty_gz_data)
        begin
          # here doesn't check whether target table is item table or not because import-only user can't read the table status.
          # So I use empty import request to check table existence.
          @client.import(database, table, "msgpack.gz", io, io.size)
          @table_list[key] = true
        rescue TreasureData::NotFoundError
          args = self.class == TreasureDataItemOutput ? ' -t item' : ''
          raise "Table #{key.inspect} does not exist on Treasure Data. Use 'td table:create #{database} #{table}#{args}' to create it."
        rescue => e
          log.warn "failed to check table existence on Treasure Data", :error => e.inspect
          log.debug_backtrace e
        end
      end
    end

    def write(chunk)
      unique_id = chunk.unique_id
      database, table = chunk.key.split('.', 2)

      FileUtils.mkdir_p(@tmpdir) unless @tmpdir.nil?
      f = Tempfile.new(@tmpdir_prefix, @tmpdir)
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

    # assume @client and @auto_create_table variable exist
    def upload(database, table, io, size, unique_id)
      unique_str = unique_id.unpack('C*').map {|x| "%02x" % x }.join
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
        ne = RuntimeError.new("Failed to upload to TreasureData: #{e} (#{size} bytes; #{elapsed} seconds)")
        ne.set_backtrace(e.backtrace)
        raise ne
      end
    end
  end
end
