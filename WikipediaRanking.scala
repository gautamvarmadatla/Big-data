package wikipedia

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Wikipedia")

  val sc: SparkContext = new SparkContext(conf)

  // TASK 1 //////////////////////////////////////////////////////////////////////

  val wikiRdd: RDD[WikipediaArticle] = ???


  // TASK 2 //////////////////////////////////////////////////////////////////////

  // TASK 2: attempt #1 ----------------------------------------------------------

  /** Returns the number of articles in which the language `lang` occurs.
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = ???


  /* Uses `occurrencesOfLang` to compute the ranking of the languages
   * (`val langs`) by determining the number of Wikipedia articles that
   * mention each language at least once.
   *
   * IMPORTANT: The result is sorted by number of occurrences, in descending order.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = ???


  // TASK 2: attempt #2 ----------------------------------------------------------

  /* Computes an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] =

    // collection of all pairs (l, wa), where l is a language and wa is a Wikipedia article.
    val pairs: RDD[(String,WikipediaArticle)] = ???

    // collection of all pairs (l, wa) where wa is an article that mentions language l.
    val mentionedPairs: RDD[(String,WikipediaArticle)] = ???
     // Hint: use `filter` and `mentionsLanguage`

    ??? // <<<<  replace ??? with the expression you want this function to return

  /* Computes the language ranking using the inverted index.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = ???


  // TASK 2: attempt #3 ----------------------------------------------------------

  /* Creates a list of (lang, integer) pairs containing one pair (l, 1) for each Wikipedia
   * article in which language l occurs.
   */
  def zipLangWithPoint(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Int)] = ???


  /* Uses `reduceByKey` to compute the index and the ranking simultaneously.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = ???



  def main(args: Array[String]): Unit =

    Logger.getLogger("org").setLevel(Level.ERROR)

    val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)

    //println("Hello world! The u.data file has " + numLines + " lines.")

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] =
      timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)]
      = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)]
      = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()


  // Do not edit `timing` or `timed`.
  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }

}

