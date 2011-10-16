require 'fluent/test'
require 'fluent/plugin/out_tdlog'

class TreasureDataLogOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  TMP_DIR = File.dirname(__FILE__) + "/tmp"

  CONFIG = %[
    apikey testkey
    buffer_path #{TMP_DIR}/buffer
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::TreasureDataLogOutput) do
      def start
        super
      end

      def write(chunk)
        chunk.read
      end
    end.configure(conf)
  end

  def test_emit
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({"a"=>1}, time)
    d.emit({"a"=>2}, time)
    d.run
  end
end

