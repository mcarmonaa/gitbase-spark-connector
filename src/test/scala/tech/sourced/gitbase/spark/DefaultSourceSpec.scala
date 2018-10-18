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

}
