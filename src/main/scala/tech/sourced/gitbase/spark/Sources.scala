package tech.sourced.gitbase.spark

import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

/**
  * Defines the hierarchy between data sources.
  */
object Sources {

  val SourceKey: String = "source"

  /** Sources ordered by their position in the hierarchy. */
  val orderedSources = Array(
    "repositories",
    "refs",
    "ref_commits",
    "commits",
    "commit_trees",
    "commit_blobs",
    "commit_files",
    "tree_entries",
    "blobs",
    "files"
  )

  /**
    * Compares two sources.
    *
    * @param a first source
    * @param b second source
    * @return comparison result
    */
  def compare(a: String, b: String): Int = orderedSources.indexOf(a)
    .compareTo(orderedSources.indexOf(b))

  def addToSchema(schema: StructType, table: String): StructType = {
    val metadata = new MetadataBuilder().putString(Sources.SourceKey, table).build()
    StructType(schema.map(f => {
      StructField(f.name, f.dataType, f.nullable, metadata)
    }))
  }

}
