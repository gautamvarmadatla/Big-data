package timeusage

import org.apache.spark.sql.{ColumnName, DataFrame, Row}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll {

//class TimeUsageSuite extends munit.FunSuite {
  import TimeUsage._

  def testOutput(): Unit = {
    val (columns, initDf) = read("timeusage/atussum.csv")
    val (primaryNeedsColumns, workColumns, otherColumns) = classifiedColumns(columns)

    val TUS = timeUsageSummary(primaryNeedsColumns, workColumns, otherColumns, initDf)
    println(" ------- timeUsageSummary ------------------------")
    TUS.show()

    val TUG = timeUsageGrouped(TUS)
    println(" ------- timeUsageGrouped ------------------------")
    TUG.show()

    val TUG_SQL = timeUsageGroupedSql(TUS)
    println(" ------- timeUsageGroupedSql ------------------------")
    TUG_SQL.show()

    val TUS_typed = timeUsageSummaryTyped(TUS)
    println(" ------- timeUsageSummaryTyped ------------------------")
    TUS_typed.show()

    val TUG_typed = timeUsageGroupedTyped(TUS_typed)
    println(" ------- timeUsageGroupedTyped ------------------------")
    TUG_typed.show()
  }
  testOutput()

}
