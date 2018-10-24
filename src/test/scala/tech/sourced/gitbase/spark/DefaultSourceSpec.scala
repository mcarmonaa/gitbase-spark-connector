package tech.sourced.gitbase.spark


class DefaultSourceSpec extends BaseGitbaseSpec {

  behavior of "DefaultSourceSpec"

  it should "count repositories" in {
    spark.table("repositories").count() should equal(3)
  }

  it should "perform joins and filters" in {
    val refs = spark.table("ref_commits")
    val commits = spark.table("commits")

    val df = refs
      .join(commits, Seq("repository_id", "commit_hash"))
      .filter(refs("history_index") === 0)

    df.count() should be(56)
  }

  it should "get all the repositories where a specific user contributes on HEAD reference" in {
    val df = spark.sql("SELECT refs.repository_id, commits.commit_author_name" +
      " FROM refs" +
      " NATURAL JOIN commits" +
      " WHERE refs.ref_name REGEXP '^refs/heads/HEAD/'" +
      " AND commits.commit_author_name = 'wangbo'")

    val rows = df.collect()
    rows.length should be(2)
    for (row <- rows) {
      row(0).asInstanceOf[String] should be("fff7062de8474d10a67d417ccea87ba6f58ca81d.siva")
    }
  }

  it should "get all the HEAD references from all repositories" in {
    val df = spark.sql("SELECT refs.repository_id, refs.ref_name" +
      " FROM refs" +
      " WHERE refs.ref_name REGEXP '^refs/heads/HEAD/'")

    df.count() should be(5)
  }

  it should "get the files in the first commit on HEAD history for all repositories" in {
    val df = spark.sql("SELECT" +
      " file_path," +
      " repository_id" +
      " FROM commit_files f" +
      " NATURAL JOIN ref_commits r" +
      " WHERE r.ref_name REGEXP '^refs/heads/HEAD/'" +
      " AND r.history_index = 0")

    df.count() should be(459)
  }

  it should "get commits that appear in more than one reference" in {
    val df = spark.sql("SELECT * FROM (" +
      " SELECT COUNT(commit_hash) AS num, commit_hash" +
      " FROM ref_commits r" +
      " NATURAL JOIN commits c" +
      " GROUP BY commit_hash" +
      ") t WHERE num > 1")

    df.count() should be(1046)
  }

  it should "get commits that appear in more than one reference without natural join" in {
    val df = spark.sql("SELECT * FROM (" +
      " SELECT COUNT(c.commit_hash) AS num, c.commit_hash" +
      " FROM ref_commits r" +
      " INNER JOIN commits c" +
      " ON c.commit_hash = r.commit_hash" +
      " AND c.repository_id = r.repository_id" +
      " GROUP BY c.commit_hash" +
      ") t WHERE num > 1")

    df.count() should be(1046)
  }

  it should "get the number of blobs per head commit" in {
    val df = spark.sql("SELECT COUNT(c.commit_hash), c.commit_hash" +
      " FROM ref_commits as r" +
      " INNER JOIN commits c" +
      "   ON r.commit_hash = c.commit_hash" +
      "   AND r.repository_id = c.repository_id" +
      " INNER JOIN commit_blobs cb" +
      "   ON cb.commit_hash = c.commit_hash" +
      "   AND cb.repository_id = c.repository_id" +
      " WHERE r.ref_name REGEXP '^refs/heads/HEAD/'" +
      " GROUP BY c.commit_hash")

    df.count() should be(985)
  }

  it should "get commits per commiter per month, in 2015" in {
    val df = spark.sql(
      """
        |SELECT COUNT(*) as num_commits, month, repo_id, committer_email
        |FROM (
        |    SELECT
        |        MONTH(committer_when) as month,
        |        r.repository_id as repo_id,
        |        committer_email
        |    FROM ref_commits r
        |    INNER JOIN commits c
        |            ON r.commit_hash = c.commit_hash
        |            AND r.repository_id = c.repository_id
        |    WHERE r.ref_name REGEXP '^refs/heads/HEAD/'
        |    AND YEAR(c.committer_when) = 2015
        |) as t
        |GROUP BY committer_email, month, repo_id
      """.stripMargin
    )

    df.count() should be(30)
  }

  it should "get files from first 6 commits from HEAD references that contain" +
    " some key and are not in vendor directory" in {
    // TODO(erizocosmico): is_binary is not supported yet
    val df = spark.sql(
      """
        |select
        |    file_path,
        |    repository_id,
        |    blob_content
        |FROM
        |    files
        |NATURAL JOIN
        |    commit_files
        |NATURAL JOIN
        |    ref_commits
        |WHERE
        |    ref_name REGEXP '^refs/heads/HEAD/'
        |    AND history_index BETWEEN 0 AND 5
        |    AND file_path NOT REGEXP '^vendor.*'
        |    AND (
        |        blob_content REGEXP '(?i)facebook.*[\'\\"][0-9a-f]{32}[\'\\"]'
        |        OR blob_content REGEXP '(?i)twitter.*[\'\\"][0-9a-zA-Z]{35,44}[\'\\"]'
        |        OR blob_content REGEXP '(?i)github.*[\'\\"][0-9a-zA-Z]{35,40}[\'\\"]'
        |        OR blob_content REGEXP 'AKIA[0-9A-Z]{16}'
        |        OR blob_content REGEXP '(?i)reddit.*[\'\\"][0-9a-zA-Z]{14}[\'\\"]'
        |        OR blob_content REGEXP '(?i)heroku.*[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}'
        |        OR blob_content REGEXP '.*-----BEGIN PRIVATE KEY-----.*'
        |        OR blob_content REGEXP '.*-----BEGIN RSA PRIVATE KEY-----.*'
        |        OR blob_content REGEXP '.*-----BEGIN DSA PRIVATE KEY-----.*'
        |        OR blob_content REGEXP '.*-----BEGIN OPENSSH PRIVATE KEY-----.*'
        |    )
      """.stripMargin)

    df.count() should be(0) // TODO(erizocosmico): this might not be correct
  }

  it should "extract information from uast udfs" in {
    val df = spark.sql("SELECT file_path," +
      " uast_extract(uast(blob_content, language(file_path, blob_content), \"//FuncLit\")," +
      "  \"internalRole\")" +
      " FROM files")
    df.count() should be(89214)
  }

}
