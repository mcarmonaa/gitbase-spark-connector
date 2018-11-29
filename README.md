# gitbase-spark-connector [![Build Status](https://travis-ci.org/src-d/gitbase-spark-connector.svg?branch=master)](https://travis-ci.org/src-d/gitbase-spark-connector) [![codecov](https://codecov.io/gh/src-d/gitbase-spark-connector/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/gitbase-spark-connector)

**gitbase-spark-connector** is a [Scala](https://www.scala-lang.org/) library that lets you expose [gitbase](https://www.github.com/src-d/gitbase) tables as [Spark SQL](https://spark.apache.org/sql/) Dataframes to run scalable analysis and processing pipelines on source code.

## Pre-requisites

* [Scala](https://www.scala-lang.org/) 2.11.12
* [Apache Spark 2.3.2 Installation](http://spark.apache.org/docs/2.3.2)
* [gitbase](https://github.com/src-d/gitbase) >= v0.18.x
* [bblfsh](https://github.com/bblfsh/bblfshd) >= 2.9.1

## Import as a dependency

For the moment, it is served through jitpack so you can check out examples about how to import it in your project [here](https://jitpack.io/#src-d/gitbase-spark-connector).

## Usage

First of all, you'll need a [gitbase](https://www.github.com/src-d/gitbase) instance running. It will expose your repositories through a SQL interface.

```
docker run -d --name gitbase -p 3306:3306 -v /path/to/repos/directory:/opt/repos srcd/gitbase:v0.17.0
```

Note you must change `/path/to/repos/directory` to the actual path where your git repositories are located.

Also, a [bblfsh](https://github.com/bblfsh/bblfshd) server could be needed for some operations on [UASTs](https://docs.sourced.tech/babelfish/uast/uast-v2)

```
docker run -d --name bblfshd --privileged -p 9432:9432 bblfsh/bblfshd:v2.9.1-drivers
```

You can configure where **gitbase** and **bblfsh** are listening by the environment variables:
- `BBLFSH_HOST` (default: "0.0.0.0")
- `BBLFSH_PORT` (default: "9432")
- `GITBASE_SERVERS` (default: "0.0.0.0:3306")

Finally you can add the gitbase `DataSource` and configuration just registering in the spark session.

```scala
import tech.sourced.gitbase.spark.GitbaseSessionBuilder

val spark = SparkSession.builder().appName("test")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .registerGitbaseSource()
    .getOrCreate()

val refs = spark.table("ref_commits")
val commits = spark.table("commits")

val df = refs
  .join(commits, Seq("repository_id", "commit_hash"))
  .filter(refs("history_index") === 0)

df.select("ref_name", "commit_hash", "committer_when").show(false)
```

Output:
```
+-------------------------------------------------------------------------------+----------------------------------------+-------------------+
|ref_name                                                                       |commit_hash                             |committer_when     |
+-------------------------------------------------------------------------------+----------------------------------------+-------------------+
|refs/heads/HEAD/015dcc49-9049-b00c-ba72-b6f5fa98cbe7                           |fff7062de8474d10a67d417ccea87ba6f58ca81d|2015-07-28 08:39:11|
|refs/heads/HEAD/015dcc49-90e6-34f2-ac03-df879ee269f3                           |fff7062de8474d10a67d417ccea87ba6f58ca81d|2015-07-28 08:39:11|
|refs/heads/develop/015dcc49-9049-b00c-ba72-b6f5fa98cbe7                        |880653c14945dbbc915f1145561ed3df3ebaf168|2015-08-19 01:02:38|
|refs/heads/HEAD/015da2f4-6d89-7ec8-5ac9-a38329ea875b                           |dbfab055c70379219cbcf422f05316fdf4e1aed3|2008-02-01 16:42:40|
+-------------------------------------------------------------------------------+----------------------------------------+-------------------+
```

## License

Apache License 2.0, see [LICENSE](/LICENSE)
