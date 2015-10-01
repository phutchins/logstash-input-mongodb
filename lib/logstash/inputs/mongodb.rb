# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/timestamp"
require "stud/interval"
require "socket" # for Socket.gethostname
require "json"
require "mongo"

include Mongo

class LogStash::Inputs::MongoDB < LogStash::Inputs::Base
  config_name "mongodb"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # Example URI: mongodb://mydb.host:27017/mydbname?ssl=true
  config :uri, :validate => :string, :required => true

  # The directory that will contain the sqlite database file.
  config :placeholder_db_dir, :validate => :string, :required => true

  # The name of the sqlite databse file
  config :placeholder_db_name, :validate => :string, :default => "logstash_sqlite.db"

  # Any table to exclude by name
  config :exclude_tables, :validate => :array, :default => []

  config :batch_size, :avlidate => :number, :default => 30

  config :since_table, :validate => :string, :default => "logstash_since"

  # The collection to use. Is turned into a regex so 'events' will match 'events_20150227'
  # Example collection: events_20150227 or events_
  config :collection, :validate => :string, :required => true

  # This allows you to select the method you would like to use to parse your data
  config :parse_method, :validate => :string, :default => 'flatten'

  # If not flattening you can dig to flatten select fields
  config :dig_fields, :validate => :array, :default => []

  # This is the second level of hash flattening
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

  config :unpack_mongo_id, :validate => :boolean, :default => false

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
  def init_placeholder(sqlitedb, since_table, mongodb, mongo_collection_name)
    @logger.debug("init placeholder for #{since_table}_#{mongo_collection_name}")
    since = sqlitedb[SINCE_TABLE]
    mongo_collection = mongodb.collection(mongo_collection_name)
    first_entry = mongo_collection.find({}).sort('_id' => 1).limit(1).first
    first_entry_id = first_entry['_id'].to_s
    since.insert(:table => "#{since_table}_#{mongo_collection_name}", :place => first_entry_id)
    return first_entry_id
  end

  public
  def get_placeholder(sqlitedb, since_table, mongodb, mongo_collection_name)
    since = sqlitedb[SINCE_TABLE]
    x = since.where(:table => "#{since_table}_#{mongo_collection_name}")
    if x[:place].nil? || x[:place] == 0
      first_entry_id = init_placeholder(sqlitedb, since_table, mongodb, mongo_collection_name)
      @logger.debug("FIRST ENTRY ID for #{mongo_collection_name} is #{first_entry_id}")
      return first_entry_id
    else
      @logger.debug("placeholder already exists, it is #{x[:place]}")
      return x[:place][:place]
    end
  end

  public
  def update_placeholder(sqlitedb, since_table, mongo_collection_name, place)
    #@logger.debug("updating placeholder for #{since_table}_#{mongo_collection_name} to #{place}")
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
    # Need to make this sort by date in object id then get the first of the series
    # db.events_20150320.find().limit(1).sort({ts:1})
    return collection.find({:_id > {:$gte > last_id_object}}).limit(batch_size)
  end

  public
  def update_watched_collections(mongodb, collection, sqlitedb)
    collections = get_collection_names(mongodb, collection)
    collection_data = {}
    collections.each do |my_collection|
      init_placeholder_table(sqlitedb)
      last_id = get_placeholder(sqlitedb, since_table, mongodb, my_collection)
      if !collection_data[my_collection]
        collection_data[my_collection] = { :name => my_collection, :last_id => last_id }
      end
    end
    return collection_data
  end

  public
  def register
    require "jdbc/sqlite3"
    require "sequel"
    placeholder_db_path = File.join(@placeholder_db_dir, @placeholder_db_name)
    mongo_uri = Mongo::URI.new(@uri)
    hosts_array = mongo_uri.servers
    db_name = mongo_uri.database
    ssl_enabled = mongo_uri.options[:ssl]
    conn = Mongo::Client.new(hosts_array, ssl: ssl_enabled, database: db_name)

    if @db_auths
      @db_auths.each do |auth|
        if !auth['db_name'].nil?
          conn.add_auth(auth['db_name'], auth['username'], auth['password'], nil)
        end
      end
      conn.apply_saved_authentication()
    end

    @host = Socket.gethostname
    @logger.info("Registering MongoDB input")

    @mongodb = conn.database
    @sqlitedb = Sequel.connect("jdbc:sqlite:#{placeholder_db_path}")

    # Should check to see if there are new matching tables at a predefined interval or on some trigger
    @collection_data = update_watched_collections(@mongodb, @collection, @sqlitedb)
  end # def register

  class BSON::OrderedHash
    def to_h
      inject({}) { |acc, element| k,v = element; acc[k] = (if v.class == BSON::OrderedHash then v.to_h else v end); acc }
    end

    def to_json
      JSON.parse(self.to_h.to_json, :allow_nan => true)
    end
  end

  def flatten(my_hash)
    new_hash = {}
    @logger.debug("Raw Hash: #{my_hash}")
    if my_hash.respond_to? :each
      my_hash.each do |k1,v1|
        if v1.is_a?(Hash)
          v1.each do |k2,v2|
            if v2.is_a?(Hash)
              # puts "Found a nested hash"
              result = flatten(v2)
              result.each do |k3,v3|
                new_hash[k1.to_s+"_"+k2.to_s+"_"+k3.to_s] = v3
              end
              # puts "result: "+result.to_s+" k2: "+k2.to_s+" v2: "+v2.to_s
            else
              new_hash[k1.to_s+"_"+k2.to_s] = v2
            end
          end
        else
          # puts "Key: "+k1.to_s+" is not a hash"
          new_hash[k1.to_s] = v1
        end
      end
    else
      @logger.debug("Flatten [ERROR]: hash did not respond to :each")
    end
    @logger.debug("Flattened Hash: #{new_hash}")
    return new_hash
  end

  def run(queue)
    sleep_min = 0.01
    sleep_max = 5
    sleeptime = sleep_min

    begin
      @logger.debug("Tailing MongoDB")
      @logger.debug("Collection data is: #{@collection_data}")
      loop do
        @collection_data.each do |index, collection|
          collection_name = collection[:name]
          @logger.debug("collection_data is: #{@collection_data}")
          last_id = @collection_data[index][:last_id]
          #@logger.debug("last_id is #{last_id}", :index => index, :collection => collection_name)
          # get batch of events starting at the last_place if it is set
          last_id_object = BSON::ObjectId(last_id)
          cursor = get_cursor_for_collection(@mongodb, collection_name, last_id_object, batch_size)
          cursor.each do |doc|
            logdate = DateTime.parse(doc['_id'].generation_time.to_s)
            event = LogStash::Event.new("host" => @host)
            decorate(event)
            event["logdate"] = logdate.iso8601
            log_entry = doc.to_h.to_s
            log_entry['_id'] = log_entry['_id'].to_s
            event["log_entry"] = log_entry
            event["mongo_id"] = doc['_id'].to_s
            @logger.debug("mongo_id: "+doc['_id'].to_s)
            #@logger.debug("EVENT looks like: "+event.to_s)
            #@logger.debug("Sent message: "+doc.to_h.to_s)
            #@logger.debug("EVENT looks like: "+event.to_s)
            # Extract the HOST_ID and PID from the MongoDB BSON::ObjectID
            if @unpack_mongo_id
              doc_hex_bytes = doc['_id'].to_s.each_char.each_slice(2).map {|b| b.join.to_i(16) }
              doc_obj_bin = doc_hex_bytes.pack("C*").unpack("a4 a3 a2 a3")
              host_id = doc_obj_bin[1].unpack("S")
              process_id = doc_obj_bin[2].unpack("S")
              event['host_id'] = host_id.first.to_i
              event['process_id'] = process_id.first.to_i
            end

            if @parse_method == 'flatten'
              # Flatten the JSON so that the data is usable in Kibana
              flat_doc = flatten(doc)
              # Check for different types of expected values and add them to the event
              if flat_doc['info_message'] && (flat_doc['info_message']  =~ /collection stats: .+/)
                # Some custom stuff I'm having to do to fix formatting in past logs...
                sub_value = flat_doc['info_message'].sub("collection stats: ", "")
                JSON.parse(sub_value).each do |k1,v1|
                  flat_doc["collection_stats_#{k1.to_s}"] = v1
                end
              end

              flat_doc.each do |k,v|
                # Check for an integer
                @logger.debug("key: #{k.to_s} value: #{v.to_s}")
                if v.is_a? Numeric
                  event[k.to_s] = v
                elsif v.is_a? String
                  if v == "NaN"
                    event[k.to_s] = Float::NAN
                  elsif /\A[-+]?\d+[.][\d]+\z/ == v
                    event[k.to_s] = v.to_f
                  elsif (/\A[-+]?\d+\z/ === v) || (v.is_a? Integer)
                    event[k.to_s] = v.to_i
                  else
                    event[k.to_s] = v
                  end
                else
                  event[k.to_s] = v.to_s unless k.to_s == "_id" || k.to_s == "tags"
                  if (k.to_s == "tags") && (v.is_a? Array)
                    event['tags'] = v
                  end
                end
              end
            elsif @parse_method == 'dig'
              # Dig into the JSON and flatten select elements
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
            else
              # Should probably do some sanitization here and insert the doc as raw as possible for parsing in logstash
            end

            queue << event
            @collection_data[index][:last_id] = doc['_id'].to_s
          end
          # Store the last-seen doc in the database
          update_placeholder(@sqlitedb, since_table, collection_name, @collection_data[index][:last_id])
        end
        @logger.debug("Updating watch collections")
        @collection_data = update_watched_collections(@mongodb, @collection, @sqlitedb)

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
