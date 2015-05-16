package ru.yandex.spark

import Spark.sc
import org.json4s._
import org.json4s.jackson.JsonMethods._


object UsersCollecting {

  implicit val formats = DefaultFormats
  val usersPerIteration = 200

  /**
   * VM options:
   * -Dpath=path - path to local filesystem, if it's necessary to store data on disc
   * -DstartId=id - id of user, which from will bfs start
   * -Dcount=number - number of users to be loaded, (will be rounded by userPerIteration constant)
   * @param args
   */
  def main(args: Array[String]): Unit = {
    var startId: String = System.getProperty("startId")
    val count: String = System.getProperty("count")
    val storageDirectory: String = System.getProperty("path")

    var usersNeeded: Int =
      if (count != null)
        count.toInt
      else
        1

    if (startId == null || "".equals(startId))
      startId = "1"

    var loadingQueue = sc.parallelize(List[String](startId))
    var loadedIds = sc.parallelize(List[String]())
    //ElasticSearchHelper.init()


    for (i <- 0 until Int.MaxValue) {

      val idsToLoad = sc.parallelize(loadingQueue.take(usersPerIteration))

      val currentUsers = idsToLoad.map(u => VkUser.addExtraInformation(User(u))).filter(_ != null)
      loadedIds = loadedIds.union(idsToLoad)

      val actualLoadedSize = currentUsers.count()

      val friends = currentUsers.flatMap(_.friends).distinct()
      loadingQueue = loadingQueue.union(friends.map(_.toString))
      loadingQueue = loadingQueue.subtract(loadedIds)

      //currentUsers.foreach(u =>  ElasticSearchHelper.addUser(u))
      if (storageDirectory != null)
        currentUsers.foreach(u => VkUser.storeUser(u, storageDirectory))

      println(actualLoadedSize + " more users added to index")
      usersNeeded -= actualLoadedSize;
      if (usersNeeded <= 0) {
        println("success!")
        return
      }
    }
    println("success!")
  }
}
