package org.examples.spark.streaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.twitter._
import org.apache.spark.storage.StorageLevel
 
  /*
 * A Spark Streaming application that receives tweets on certain 
 * keywords from twitter datasource and find the popular hashtags
 * 
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <keyword_1> ... <keyword_n>
 * <comsumerKey>        - Twitter consumer key 
 * <consumerSecret>     - Twitter consumer secret
 * <accessToken>        - Twitter access token
 * <accessTokenSecret>  - Twitter access token secret
 * <keyword_1>          - The keyword to filter tweets
 * <keyword_n>          - Any number of keywords to filter tweets
 */
  

object TwitterPopularHashTags {
 
  val conf = new SparkConf().setAppName("Twitter Popular Hashtags")
      .setMaster("local[4]")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularHashTags <ck> <cs> <at> <ats> [keywords]")
      System.exit(1)
    }
    
    sc.setLogLevel("WARN")
    
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)
    
    // Setting the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    
    val ssc = new StreamingContext(sc, Seconds(5))
    val stream = TwitterUtils.createStream(ssc, None, filters)
    
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    
    val topCounts60 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_, Seconds(60))
        .map{ case (topic,count) => (count,topic) }
        .transform(_.sortByKey(false))
        
    val topCounts10 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_, Seconds(10))
        .map{ case (topic,count) => (count,topic) }
        .transform(_.sortByKey(false))
    
    stream.print()
    
    topCounts60.foreachRDD( rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total): ".format(rdd.count()))
      topList.foreach{ case (count,tag) => println("%s (%s tweets)".format(tag, count)) }   
    })
    
    topCounts10.foreachRDD( rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total): ".format(rdd.count()))
      topList.foreach{ case (count,tag) => println("%s (%s tweets)".format(tag, count)) }   
    })
    
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}