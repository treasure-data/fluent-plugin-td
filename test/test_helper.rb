require 'json'
require 'msgpack'
require 'fluent/test'
require 'webmock/test_unit'
require 'stringio'
require 'td-client'
require 'zlib'

def e(s)
  require 'cgi'
  CGI.escape(s.to_s)
end

class Test::Unit::TestCase
  def create_too_many_keys_record
    record = {}
    5012.times { |i| record["k#{i}"] = i }
    record
  end

  def stub_seed_values
    time = Time.parse("2014-01-01 00:00:00 UTC").to_i
    records = [{"a" => 1}, {"a" => 2}]
    return time, records
  end

  def stub_request_body(records, time = nil)
    out = ''
    records.each { |record|
      r = record.dup
      if time
        r['time'] = time
      end
      r.to_msgpack(out)
    }

    io = StringIO.new
    gz = Zlib::GzipWriter.new(io)
    FileUtils.copy_stream(StringIO.new(out), gz)
    gz.finish
    io.string
  end

  def stub_gzip_unwrap(body)
    io = StringIO.new(body)
    gz = Zlib::GzipReader.new(io)
    gz.read
  end

  def stub_td_table_create_request(database, table, opts = {})
    opts[:use_ssl] = true unless opts.has_key?(:use_ssl)
    schema = opts[:use_ssl] ? 'https' : 'http'
    response = {"database" => database, "table" => table}.to_json
    endpoint = opts[:endpoint] ? opts[:endpoint] : TreasureData::API::NEW_DEFAULT_ENDPOINT

    url = "#{schema}://#{endpoint}/v3/table/create/#{e(database)}/#{e(table)}/log"
    stub_request(:post, url).to_return(:status => 200, :body => response)
  end

  def stub_td_import_request(body, db, table, opts = {})
    opts[:use_ssl] = true unless opts.has_key?(:use_ssl)
    format = opts[:format] || 'msgpack.gz'
    schema = opts[:use_ssl] ? 'https' : 'http'
    response = {"database" => db, "table" => table, "elapsed_time" => 0}.to_json
    endpoint = opts[:endpoint] ? opts[:endpoint] : TreasureData::API::NEW_DEFAULT_IMPORT_ENDPOINT

    # for check_table_existence
    url_with_empty = "#{schema}://#{endpoint}//v3/table/import/#{e(db)}/#{e(table)}/#{format}"
    stub_request(:put, url_with_empty).to_return(:status => 200, :body => response)

    url_with_unique = Regexp.compile("#{schema}://#{endpoint}//v3/table/import_with_id/#{e(db)}/#{e(table)}/.*/#{format}")
    stub_request(:put, url_with_unique).with(:headers => {'Content-Type' => 'application/octet-stream'}) { |req|
      @auth_header = req.headers["Authorization"]
      stub_gzip_unwrap(req.body) == stub_gzip_unwrap(body)
    }.to_return(:status => 200, :body => response)
  end
end
