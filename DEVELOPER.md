# logstash-input-example
Example input plugin. This should help bootstrap your effort to write your own input plugin!

## Build new gem

Change version in logstash-input-mongodb.gemspec

And build new gem:

```bash
$ gem build logstash-input-mongodb.gemspec
```

## Developing

### Use rvm on Ubuntu

1. Install rvm https://rvm.io/

2. Install jruby

```bash
$ rvm install jruby 1.7.25
```

3. Use jruby from rvm

```bash
$ rvm alias create default jruby-1.7.25
$ rvm use default
$ /bin/bash --login
$ gem install bundler
$ bundle install
```

4. Run tests

* Unit
```bash
$ ruby test/*
```

* Rspec (but it needs mongo on localhost, it would be nice to have it is not depended on local mongo)
```bash
$ ./bin/rspec
```
