require 'fluent/test'
require 'fluent/test/driver/output'
require 'fluent/plugin/out_tdlog'
require 'test_helper'

class TreasureDataLogOutputTest < Test::Unit::TestCase
  TMP_DIR = File.dirname(__FILE__) + "/tmp"

  def setup
    super
    Fluent::Test.setup
    FileUtils.rm_rf(TMP_DIR, secure: true)
    FileUtils.mkdir_p(TMP_DIR)
  end

  def teardown
    super
    Fluent::Engine.stop
  end

  BASE_CONFIG = %[
    apikey testkey
    buffer_path #{TMP_DIR}/buffer
  ]
  DEFAULT_CONFIG = %[
    database test
    table table
  ]

  def create_driver(conf = DEFAULT_CONFIG)
    config = BASE_CONFIG + conf

    Fluent::Test::Driver::Output.new(Fluent::Plugin::TreasureDataLogOutput) do
      def write(chunk)
        chunk.instance_variable_set(:@key, @key)
        def chunk.key
          @key
        end
        super(chunk)
      end
    end.configure(config)
  end

  def test_configure
    d = create_driver

    {:@apikey => 'testkey', :@use_ssl => true, :@auto_create_table => true, :@use_gzip_command => false}.each { |k, v|
      assert_equal(d.instance.instance_variable_get(k), v)
    }
    {:@chunk_keys => ['tag'], :@flush_interval => 300, :@chunk_limit_size => Fluent::Plugin::TreasureDataLogOutput::IMPORT_SIZE_LIMIT}.each { |k, v|
      assert_equal(d.instance.buffer.instance_variable_get(k), v)
    }
  end

  def test_configure_for_chunk_key_tag
    assert_raise Fluent::ConfigError.new("'tag' must be included in <buffer ARG> when database and table are not specified") do
      Fluent::Test::Driver::Output.new(Fluent::Plugin::TreasureDataLogOutput).configure(%[
        apikey testkey
        <buffer []>
          flush_interval 10s
          path #{TMP_DIR}/buffer
        </buffer>
      ])
    end
  end

  data('evet_time' => 'event_time', 'int_time' => 'int')
  def test_emit(time_class)
    d = create_driver
    time, records = stub_seed_values(time_class)
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table)
    assert_rr {
      # mock(d.instance).gzip_by_writer(is_a(Fluent::BufferChunk), is_a(Tempfile)) causes empty request body so using dont_allow instead to check calling method
      # We need actual gzipped content to verify compressed body is correct or not.
      dont_allow(d.instance).gzip_by_command(anything, is_a(Tempfile))

      d.run(default_tag: 'test') {
        records.each { |record|
          d.feed(time, record)
        }
      }
    }

    assert_equal('TD1 testkey', @auth_header)
  end

  def test_emit_with_gzip_command
    omit "On Windows, `use_gzip_command` is not available." if Fluent.windows?
    d = create_driver(DEFAULT_CONFIG + "use_gzip_command true")
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table)
    assert_rr {
      # same as test_emit
      dont_allow(d.instance).gzip_by_writer(anything, is_a(Tempfile))
      d.run(default_tag: 'test') {
        records.each { |record|
          d.feed(time, record)
        }
      }
    }

    assert_equal('TD1 testkey', @auth_header)
  end

  def test_emit_with_broken_record
    d = create_driver
    time, records = stub_seed_values
    records[1] = nil
    records << 'string' # non-hash case
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table)

    d.run(default_tag: 'test') {
      d.feed_to_plugin('test', Fluent::ArrayEventStream.new(records.map { |e| [time, e] }))
    }

    error_events = d.error_events(tag: 'test')
    assert_equal 2, error_events.size
    assert_equal nil, error_events[0][2]['record']
    assert_equal "string", error_events[1][2]['record']
  end

  def test_emit_with_bigint_record
    n = 100000000000000000000000
    d = create_driver
    time, records = stub_seed_values
    records[1]['k'] = ['hogehoge' * 1000]
    records[1]['kk'] = n.to_s # bigint is converted to string
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table)

    test_time, test_records = stub_seed_values
    test_records[1]['k'] = ['hogehoge' * 1000]
    test_records[1]['kk'] = n
    d.run(default_tag: 'test') {
      test_records.each { |record|
        d.feed(test_time, record)
      }
    }
  end

  def test_emit_with_time_symbol
    d = create_driver
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table)

    d.run(default_tag: 'test') {
      records.each { |record|
        record[:time] = Time.now.to_i  # emit removes this :time key
        d.feed(time, record)
      }
    }

    assert_equal('TD1 testkey', @auth_header)
  end

  def test_emit_with_endpoint
    d = create_driver(DEFAULT_CONFIG + "endpoint foo.bar.baz\napi_endpoint boo.bar.baz")
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table, {:endpoint => 'boo.bar.baz'})
    stub_td_import_request(stub_request_body(records, time), database, table, {:endpoint => 'foo.bar.baz'})

    d.run(default_tag: 'test') {
      records.each { |record|
        d.feed(time, record)
      }
    }
  end

  def test_emit_with_too_many_keys
    d = create_driver(DEFAULT_CONFIG)
    time, _ = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body([], time), database, table)

    d.run(default_tag: 'test') {
      d.feed(time, create_too_many_keys_record)
    }

    assert_equal 0, d.events.size
    assert_equal 1, d.error_events.size
  end

  def test_write_with_client_error
    d = create_driver(DEFAULT_CONFIG)
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table, status: 400)

    assert_nothing_raised(Fluent::UnrecoverableError) do
      d.run(default_tag: 'test') {
        records.each { |record|
          d.feed(time, record)
        }
      }
    end
  end

  def test_write_retry_if_too_many_requests
    d = create_driver(DEFAULT_CONFIG)
    time, records = stub_seed_values
    database, table = d.instance.instance_variable_get(:@key).split(".", 2)
    stub_td_table_create_request(database, table)
    stub_td_import_request(stub_request_body(records, time), database, table, status: 429)

    assert_raise(TreasureData::TooManyRequestsError) do
      d.run(default_tag: 'test') {
        records.each { |record|
          d.feed(time, record)
        }
      }
    end
  end

  sub_test_case 'tag splitting for database and table' do
    def create_driver(conf = %[auto_create_table true])
      config = BASE_CONFIG + conf

      Fluent::Test::Driver::Output.new(Fluent::Plugin::TreasureDataLogOutput).configure(config)
    end

    data('event_time' => 'event_time', 'int_time' => 'int')
    def test_tag_split(time_class)
      d = create_driver

      time, records = stub_seed_values(time_class)
      database = 'db1'
      table = 'table1'
      stub_td_table_create_request(database, table)
      stub_td_import_request(stub_request_body(records, time), database, table)

      d.run(default_tag: 'td.db1.table1') {
        records.each { |record|
          d.feed(time, record)
        }
      }
    end

    def test_tag_split_with_normalization
      d = create_driver

      time, records = stub_seed_values
      database = 'db_'
      table = 'tb_'
      stub_td_table_create_request(database, table)
      stub_td_import_request(stub_request_body(records, time), database, table)

      d.run(default_tag: 'td.db.tb') {
        records.each { |record|
          d.feed(time, record)
        }
      }
    end
  end
end

