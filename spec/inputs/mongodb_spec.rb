# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "tempfile"
require "stud/temporary"
require "logstash/inputs/mongodb"
require 'mongo'
include Mongo

FILE_DELIMITER = LogStash::Environment.windows? ? "\r\n" : "\n"

describe LogStash::Inputs::MongoDB do
  collection = 'test_events'
  mongo_uri = 'mongodb://localhost/logstash-input-mongodb_test'
  sqlite_db_file = Stud::Temporary.file
  placeholder_db_dir = File.dirname sqlite_db_file
  let(:settings) { {} }
  let(:plugin) { LogStash::Inputs::MongoDB.new(settings) }
  let(:queue) { Queue.new }
  let (:db) do
    Mongo::Client.new(mongo_uri).database
  end

  before :each do
    @abort_on_exception = Thread.abort_on_exception

    # Create the test DB and populate it with some data
    coll = db[collection]
    coll.insert_one({:message => "first message"})
    coll.insert_one({:message => "second message"})
    coll.insert_one({:message => "third message"})
    coll.insert_one({:message => "fourth message"})
  end

  after :each do
    coll = db.collection(collection)
    coll.drop
  end

  context "when no sincedb data exists" do
    let(:settings) do
      {
        "uri" => mongo_uri,
        "river_mode" => true,
        "placeholder_db_dir" => placeholder_db_dir,
        "collection" => collection
      }
    end

    before do
      plugin.register
      plugin.run(queue)
    end

    after do
      plugin.stop
    end

    it "should start at the beginning of a collection when no sincedb data exists" do

      expect(queue.size).to eq(4)
      expect(queue.pop.get('message')).to eq('first message')
      expect(queue.pop.get('message')).to eq('second message')
      expect(queue.pop.get('message')).to eq('third message')
      expect(queue.pop.get('message')).to eq('fourth message')

    end

  end

  context "when sincedb contains a placeholder" do
    let(:settings) do
      {
        "uri" => mongo_uri,
        "river_mode" => true,
        "placeholder_db_dir" => placeholder_db_dir,
        "collection" => collection
      }
    end

    before do
      # create sincedb file with placeholder in it
      plugin.register
      plugin.run(queue)
    end

    after do
      plugin.stop
    end

    it "should start where it left off in a collection when it has sincedb data" do

    end
  end
end
