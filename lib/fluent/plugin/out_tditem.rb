require 'td-client'
require 'fluent/plugin/td_plugin_version'

module Fluent
  class TreasureDataItemOutput < BufferedOutput
    Plugin.register_output('tditem', self)

    require_relative 'td_plugin_util'
    include TDPluginUtil

    IMPORT_SIZE_LIMIT = 32 * 1024 * 1024

    # To support log_level option since Fluentd v0.10.43
    unless method_defined?(:log)
      define_method(:log) { $log }
    end

    config_param :apikey, :string
    config_param :database, :string
    config_param :table, :string
    config_param :tmpdir, :string, :default => nil
    #config_param :auto_create_table, :bool, :default => true # TODO: implement if user wants this feature

    config_param :endpoint, :string, :default => TreasureData::API::NEW_DEFAULT_ENDPOINT
    config_param :use_ssl, :bool, :default => true
    config_param :http_proxy, :string, :default => nil
    config_param :connect_timeout, :integer, :default => nil
    config_param :read_timeout, :integer, :default => nil
    config_param :send_timeout, :integer, :default => nil
    config_set_default :buffer_type, 'file'
    config_set_default :flush_interval, 300

    def initialize
      super

      @auto_create_table = false
      @tmpdir_prefix = 'tditem-'.freeze
      @key_num_limit = 1024  # Item table default limitation
      @record_size_limit = 32 * 1024 * 1024  # TODO
      @empty_gz_data = TreasureData::API.create_empty_gz_data
      @user_agent = "fluent-plugin-td-item: #{TreasureDataPlugin::VERSION}".freeze
    end

    def configure(conf)
      super

      # overwrite default value of buffer_chunk_limit
      if @buffer.respond_to?(:buffer_chunk_limit=) && !conf.has_key?('buffer_chunk_limit')
        @buffer.buffer_chunk_limit = IMPORT_SIZE_LIMIT
      end

      validate_database_and_table_name(@database, @table, conf)
      @key = "#{@database}.#{@table}".freeze
      @use_ssl = parse_bool_parameter(@use_ssl) if @use_ssl.instance_of?(String)
      FileUtils.mkdir_p(@tmpdir) unless @tmpdir.nil?
    end

    def start
      super

      client_opts = {
        :ssl => @use_ssl, :http_proxy => @http_proxy, :user_agent => @user_agent, :endpoint => @endpoint,
        :connect_timeout => @connect_timeout, :read_timeout => @read_timeout, :send_timeout => @send_timeout
      }
      @client = TreasureData::Client.new(@apikey, client_opts)

      check_table_existence(@database, @table)
    end

    def emit(tag, es, chain)
      super(tag, es, chain, @key)
    end

    def format_stream(tag, es)
      out = ''
      off = out.bytesize
      es.each { |time, record|
        if record.size > @key_num_limit
          log.error "Too many number of keys (#{record.size} keys)" # TODO include summary of the record
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
  end
end
