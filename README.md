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

# running the tool

Use the ```bin/kinesis-datagen``` script. Use the ```-help``` option to learn about the command-line parameters.

# nasa http datasets

You can download the NASA http logs from http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

The logs are an ASCII file with one line per request, with the following columns:

1. host making the request. A hostname when possible, otherwise the Internet address if the name could not be looked up.
2. timestamp in the format "DAY MON DD HH:MM:SS YYYY", where DAY is the day of the week, MON is the name of the month, DD is the day of the month, HH:MM:SS is the time of day using a 24-hour clock, and YYYY is the year. The timezone is -0400.
3. request given in quotes.
4. HTTP reply code.
5. bytes in the reply.

The tool will read this data and push it into the target kinesis stream.
