# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "tempfile"
require "stud/temporary"
require "logstash/inputs/mongodb"

FILE_DELIMITER = LogStash::Environment.windows? ? "\r\n" : "\n"

describe LogStash::Inputs::Mongodb do
  before(:all) do
    @abort_on_exception = Thread.abort_on_exception
    Thread.abort_on_exception = true
  end

  after(:all) do
    Thread.abort_on_exception = @abort_on_exception
  end

  it_behaves_like "an interruptible input plugin" do
    let(:config) do
      {
        placeholder_db_dir => Stud::Temporary.pathname,
        placeholder_db_bame => Stud::Temporary.file,
        collection => 'logstash-input-mongodb_test'
      }
    end
  end

  it "should start at the beginning of a collection when no sincedb data exists" do
    placeholder_db_dir = Stud::Temporary.pathname
    placeholder_db_name = Stud::Temporary.pathname
    collection = 'logstash-input-mongodb_test'

    conf = <<-CONFIG
      input {
        mongodb {
          uri => 'mongodb://localhost/logstash-input-mongodb_test',
          placeholder_db_dir => "#{placeholder_db_dir}"
          placeholder_db_name => "#{placeholder_db_name}"
          collection => "#{collection}"
        }
      }
    CONFIG

    # Create the test DB and populate it with some data
    # add "first message"
    # add "second message"

    events = input(conf) do |pipeline, queue|

      events = []

      retries = 0
      while retries < 20
        # Add some new entries to the database
        # add "third message"
        # add "fourth message"

        if queue.size >= 4
          events = 4.times.collect { queue.pop }
          break
        end

        sleep(0.1)
        retries += 1
      end

      events
    end

    insist { events[0]["message"] } == "first message"
    insist { events[1]["message"] } == "second message"
    insist { events[2]["message"] } == "third message"
    insist { events[3]["message"] } == "fourth message"
  end

  it "should start where it left off in a collection when it has sincedb data" do

  end
end
