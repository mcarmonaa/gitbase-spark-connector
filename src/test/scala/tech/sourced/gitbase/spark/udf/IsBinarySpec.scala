package tech.sourced.gitbase.spark.udf

class IsBinarySpec extends BaseUdfSpec {
  behavior of "is_binary UDF"

  it should "return None if array is null" in {
    IsBinary.isBinary(null) should be(None)
  }

  it should "return false if array is empty" in {
    IsBinary.isBinary(Array[Byte]()) should be(Some(false))
  }

  it should "guess a binary value" in {
    IsBinary.isBinary(Array[Byte](0, 1, 3)) should be(Some(true))
    IsBinary.isBinary(Array[Byte](1, 2, 3)) should be(Some(false))
  }
}
