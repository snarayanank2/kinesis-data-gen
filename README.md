# kinesis-data-gen

Small project that will help create and populate a kinesis stream.

# running tests

```
mvn clean test
```

# building

```
mvn clean package
```

This will create ```datagen-x.y.z-bin.tar.gz``` file. You can extract it on the machine where you want to run against larger datasets.

**Note**
> You may want to update pom.xml to refer to a matching Presto version. Sometimes there may be backward incompatible changes in API and a connector built against a certain version may not work with others.

# running the tool

Use the ```bin/kinesis-datagen``` script. Use the ```-help``` option to learn about the command-line parameters.

    usage: kinesis-datagen
	-create                      drop and recreate stream
	-f,--sample-file <file>      sample file to generate records (required)
	-help                        print this message
    -n,--kinesis-stream <name>   kinesis stream to write to (required)
	-p,--parse-only              only parse the sample file
	-r,--read-only               only read the sample file, no parsing
	-s,--num-shards <number>     number of shards
	-verbose                     be extra verbose
	-w,--num-workers <number>    number of workers (default 1)

You should see a file ``etc/sample_data.txt`` which can be used as a
sample file for the data generator. Alternatively, you can download
the http logs from NASA described below. Flags ``-r`` and ``-p`` only
read and parse the sample file and don't push data to Kinesis. They're
used primarily for debugging.

# nasa http datasets

You can download the NASA http logs from http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

The logs are an ASCII file with one line per request, with the following columns:

1. host making the request. A hostname when possible, otherwise the Internet address if the name could not be looked up.
2. timestamp in the format "DAY MON DD HH:MM:SS YYYY", where DAY is the day of the week, MON is the name of the month, DD is the day of the month, HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year. The timezone is -0400.
3. request given in quotes.
4. HTTP reply code.
5. bytes in the reply.

The tool will read this data and push it into the target kinesis stream.

