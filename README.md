# Logstash Plugin

This is a plugin for [Logstash](https://github.com/elasticsearch/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Documentation

This is a logstash plugin for pulling data out of mongodb and processing with logstash. It will connect to the database specified in `uri`, use the `collection` attribute to find collections to pull documents from, start at the first collection it finds and pull the number of documents specified in `batch_size`, save it's progress in an sqlite database who's location is specified by `placeholder_db_dir` and `placeholder_db_name` and repeat. It will continue this until it no longer finds documents newer than ones that it has processed, sleep for a moment, then continue to loop over the collections.

This was designed for parsing logs that were written into mongodb. This means that it may not re-parse db entries that were changed and already parsed.


### Installation

+ Logstash installed from ZIP | TGZ
  + bin/plugin install /path/to/logstash-input-mongodb-0.1.3.gem

+ Logstash from GIT
  + git clone https://github.com/elastic/logstash.git
  + cd logstash
  + (ensure that the correct jruby is installed for the version of logstash you are installing)
  + rake test:install-core
  + bin/plugin install /path/to/logstash-input-mongodb-0.1.3.gem
  + bin/plugin install --development

### Configuration Options

  uri: A MongoDB URI for your database or cluster (check the MongoDB documentation for further info on this) [No Default, Required]
  placeholder_db_path: Path where the place holder database will be stored locally to disk [No Default, Required]
    This gets created by the plugin so the directory needs to be writeable by the user that logstash is running as
  placeholder_db_name: Name of the database file that will be created [Default: logstash_sqlite.db]
  collection: A regex that will be used to find desired collecitons. [No Default, Required]
  batch_size: Size of the batch of mongo documents to pull at a time [Default: 30]


### Configuration

Example
```
input {
  mongodb {
    uri => 'mongodb://10.0.0.30/my-logs?ssl=true'
    placeholder_db_dir => '/opt/logstash-mongodb/'
    placeholder_db_name => 'logstash_sqlite.db'
    collection => 'events_'
    batch_size => 5000
  }
}

filter {
  date {
    match => [ "logdate", "ISO8601" ]
  }
}

output {
  redis {
    host => "localhost"
    data_type => "list"
    key => "logstash-mylogs"
  }
}
```
