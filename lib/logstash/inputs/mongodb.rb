# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "stud/interval"
require "socket" # for Socket.gethostname
require "json"
require "bson"

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::MongoDB < LogStash::Inputs::Base
  config_name "mongodb"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # Example URI: mongodb://mydb.host:27017/mydbname?ssl=true
  config :uri, :validate => :string, :required => true

  # The path to the sqlite database file.
  config :path, :validate => :string, :required => true

  # Any table to exclude by name
  config :exclude_tables, :validate => :array, :default => []

  config :batch_size, :avlidate => :number, :default => 30

  config :since_table, :validate => :string, :default => "logstash_since"

  # The collection to use. Is turned into a regex so 'events' will match 'events_20150227'
  # Example collection: events_20150227 or events_
  config :collection, :validate => :string, :required => true

  config :dig_fields, :validate => :array, :default => []

  config :dig_dig_fields, :validate => :array, :default => []

  # If true, store the @timestamp field in mongodb as an ISODate type instead
  # of an ISO8601 string.  For more information about this, see
  # http://www.mongodb.org/display/DOCS/Dates
  config :isodate, :validate => :boolean, :default => false

  # Number of seconds to wait after failure before retrying
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # If true, an "_id" field will be added to the document before insertion.
  # The "_id" field will use the timestamp of the event and overwrite an existing
  # "_id" field in the event.
  config :generateId, :validate => :boolean, :default => false

  # The message string to use in the event.
  config :message, :validate => :string, :default => "Default message..."

  # Set how frequently messages should be sent.
  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :default => 1

  SINCE_TABLE = :since_table

  public
  def init_placeholder_table(sqlitedb)
    begin
      sqlitedb.create_table "#{SINCE_TABLE}" do
        String :table
        Int :place
      end
    rescue
      @logger.debug("since table already exists")
    end
  end

  public
  def get_placeholder(sqlitedb, since_table, mongodb, mongo_collection_name)
    since = sqlitedb[SINCE_TABLE]
    x = since.where(:table => "#{since_table}_#{mongo_collection_name}")
    if x[:place].nil?
      init_placeholder(sqlitedb, since_table, mongodb, mongo_collection_name)
      return 0
    else
      @logger.debug("placeholder already exists, it is #{x[:place]}")
      return x[:place][:place]
    end
  end

  public
  def init_placeholder(sqlitedb, since_table, mongodb, mongo_collection_name)
    @logger.debug("init placeholder for #{since_table}_#{mongo_collection_name}")
    since = sqlitedb[SINCE_TABLE]
    mongo_collection = mongodb.collection(mongo_collection_name)
    first_entry = mongo_collection.find_one({})
    first_entry_id = first_entry['_id'].to_s
    since.insert(:table => "#{since_table}_#{mongo_collection_name}", :place => first_entry_id)
  end

  public
  def update_placeholder(sqlitedb, since_table, mongo_collection_name, place)
    @logger.debug("updating placeholder for #{since_table}_#{mongo_collection_name} to #{place}")
    since = sqlitedb[SINCE_TABLE]
    since.where(:table => "#{since_table}_#{mongo_collection_name}").update(:place => place)
  end

  public
  def get_all_tables(mongodb)
    return @mongodb.collection_names
  end

  public
  def get_collection_names(mongodb, collection)
    collection_names = []
    @mongodb.collection_names.each do |coll|
      if /#{collection}/ =~ coll
        collection_names.push(coll)
        @logger.debug("Added #{coll} to the collection list as it matches our collection search")
      end
    end
    return collection_names
  end

  public
  def get_cursor_for_collection(mongodb, mongo_collection_name, last_id_object, batch_size)
    collection = mongodb.collection(mongo_collection_name)
    return collection.find({:_id => {:$gt => last_id_object}}).limit(batch_size)
  end

  public
  def register
    require "mongo"
    require "jdbc/sqlite3"
    require "sequel"
    uriParsed = Mongo::URIParser.new(@uri)
    conn = uriParsed.connection({})
    if uriParsed.auths.length > 0
      uriParsed.auths.each do |auth|
        if !auth['db_name'].nil?
          conn.add_auth(auth['db_name'], auth['username'], auth['password'], nil)
        end
      end
      conn.apply_saved_authentication()
    end
    @host = Socket.gethostname
    @logger.info("Registering MongoDB input", :database => @path)
    #@mongodb = conn.db(@database)
    @mongodb = conn.db(uriParsed.db_name)
    @sqlitedb = Sequel.connect("jdbc:sqlite:#{@path}")
    # Should check to see if there are new matching tables at a predefined interval or on some trigger
    @collections = get_collection_names(@mongodb, @collection)
    @collection_data = {}
    @collections.each do |collection|
      init_placeholder_table(@sqlitedb)
      last_id = get_placeholder(@sqlitedb, since_table, @mongodb, collection)
      @collection_data[collection] = { :name => collection, :last_id => last_id }
    end
  end # def register

  class BSON::OrderedHash
    def to_h
      inject({}) { |acc, element| k,v = element; acc[k] = (if v.class == BSON::OrderedHash then v.to_h else v end); acc }
    end

    def to_json
      to_h.to_json
    end
  end

  def run(queue)
    sleep_min = 0.01
    sleep_max = 5
    sleeptime = sleep_min

    begin
      @logger.debug("Tailing MongoDB", :path => @path)
      @logger.debug("Collection data is: #{@collection_data}")
      loop do
        @collection_data.each do |index, collection|
          collection_name = collection[:name]
          @logger.debug("collection_data is: #{@collection_data}")
          last_id = @collection_data[index][:last_id]
          @logger.debug("last_id is #{last_id}", :index => index, :collection => collection_name)
          # get batch of events starting at the last_place if it is set
          last_id_object = BSON::ObjectId(last_id)
          cursor = get_cursor_for_collection(@mongodb, collection_name, last_id_object, batch_size)
          cursor.each do |doc|
            @logger.debug("Date from mongo: #{doc['_id'].generation_time.to_s}")
            logdate = DateTime.parse(doc['_id'].generation_time.to_s)
            @logger.debug("logdate.iso8601: #{logdate.iso8601}")
            event = LogStash::Event.new("host" => @host)
            decorate(event)
            event["logdate"] = logdate.iso8601
            @logger.debug("Message will be: #{JSON.parse(doc.to_json, :allow_nan => true)}")
            event["message"] = JSON.parse(doc.to_json, :allow_nan => true)
            doc.each do |k, v|
              if k != "_id"
                if (@dig_fields.include? k) && (v.respond_to? :each)
                  v.each do |kk, vv|
                    if (@dig_dig_fields.include? kk) && (vv.respond_to? :each)
                      vv.each do |kkk, vvv|
                        if /\A[-+]?\d+\z/ === vvv
                          event["#{k}_#{kk}_#{kkk}"] = vvv.to_i
                        else
                          event["#{k}_#{kk}_#{kkk}"] = vvv.to_s
                        end
                      end
                    else
                      if /\A[-+]?\d+\z/ === vv
                        event["#{k}_#{kk}"] = vv.to_i
                      else
                        event["#{k}_#{kk}"] = vv.to_s
                      end
                    end
                  end
                else
                  if /\A[-+]?\d+\z/ === v
                    event[k] = v.to_i
                  else
                    event[k] = v.to_s
                  end
                end
              end
            end
            queue << event
            @collection_data[index][:last_id] = doc['_id'].to_s
          end
          # Store the last-seen doc in the database
          update_placeholder(@sqlitedb, since_table, collection_name, @collection_data[index][:last_id])
        end

        # nothing found in that iteration
        # sleep a bit
        @logger.debug("No new rows. Sleeping.", :time => sleeptime)
        sleeptime = [sleeptime * 2, sleep_max].min
        sleep(sleeptime)
        #sleeptime = sleep_min
      end
    rescue LogStash::ShutdownSignal
      if @interrupted
        @logger.debug("Mongo Input shutting down")
      end
    end
  end # def run

end # class LogStash::Inputs::Example
