# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "tempfile"
require "stud/temporary"
require "logstash/inputs/mongodb"
require 'mongo'
include Mongo

FILE_DELIMITER = LogStash::Environment.windows? ? "\r\n" : "\n"

describe LogStash::Inputs::MongoDB do
  before(:all) do
    @abort_on_exception = Thread.abort_on_exception
    Thread.abort_on_exception = true
  end

  after(:all) do
    Thread.abort_on_exception = @abort_on_exception
  end

  it_behaves_like "an interruptible input plugin" do
    sqlite_db_file = Stud::Temporary.file
    placeholder_db_dir = File.dirname sqlite_db_file
    placeholder_db_name = File.basename sqlite_db_file
    collection = 'logstash-input-mongodb_test'
    let(:config) do
      {
        "uri" => 'mongodb://localhost/logstash-input-mongodb_test',
        "placeholder_db_dir" => "#{placeholder_db_dir}",
        "collection" => "#{collection}"
      }
    end
  end

  it "should start at the beginning of a collection when no sincedb data exists" do
    mongo_uri = 'mongodb://localhost/logstash-input-mongodb_test'
    sqlite_db_file = Stud::Temporary.file
    placeholder_db_dir = File.dirname sqlite_db_file
    placeholder_db_name = File.basename sqlite_db_file

    collection = 'logstash-input-mongodb_test'

    conf = <<-CONFIG
      input {
        mongodb {
          uri => "#{mongo_uri}"
          placeholder_db_dir => "#{placeholder_db_dir}"
          placeholder_db_name => "#{placeholder_db_name}"
          collection => "#{collection}"
        }
      }
    CONFIG

    # Create the test DB and populate it with some data

    db = Mongo::Client.new(mongo_uri).database
    coll = db.collection(collection)
    coll.drop
    coll = db.collection(collection)
    coll.insert_one({:message => "first message"})
    coll.insert_one({:message => "second message"})
    coll.insert_one({:message => "third message"})
    coll.insert_one({:message => "fourth message"})

    events = input(conf) do |pipeline, queue|

      retries = 0
      while retries < 20
        # Add some new entries to the database

        events = []
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

  xit "should start where it left off in a collection when it has sincedb data" do

  end

end
