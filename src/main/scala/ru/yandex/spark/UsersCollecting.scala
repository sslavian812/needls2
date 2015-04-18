package ru.yandex.spark

import Spark.sc
import org.json4s._
import org.json4s.jackson.JsonMethods._


object UsersCollecting {

  implicit val formats = DefaultFormats
  val usersPerIteration = 10

  /**
   * VM options:
   *     -Dpath=path - path to local filesystem, if it's necessary to store data on disc
   *     -DstartId=id - id of user, which from will bfs start
   *     -Dcount=number - number of users to be loaded, (will be rounded by userPerIteration constant)
   * @param args
   */
  def main(args: Array[String]): Unit = {
    var startId : String          = System.getProperty("startId")
    val count : String            = System.getProperty("count")
    val storageDirectory : String = System.getProperty("path")

    val iterations : Int=
    if(count != null)
      (count.toInt) / usersPerIteration + 1
    else
      1

    if(startId == null || "".equals(startId))
      startId = "1"

    var mainLoadingQueue = sc.parallelize(List[String]())
    var alreadyLoadedIds = sc.parallelize(List[String]())
    var loadOnIteration = sc.parallelize(List(startId))

    ElasticSearchHelper.init()


    for( i <- 0 until iterations) {
      println("=======" + i + "-th iteration")
      println("MLQ.size = " + mainLoadingQueue.count())

      val currentUsers = loadOnIteration.map(u => VkUser.addExtraInformation(User(u)))
      alreadyLoadedIds = alreadyLoadedIds.union(currentUsers.map(_.id))
      // load Users

      val friendsIds = currentUsers.flatMap(_.friends).distinct()
      println("friends.size = " + friendsIds.count())

      mainLoadingQueue = friendsIds.map(_.toString).subtract(alreadyLoadedIds)
      println("MLQ.size = " + mainLoadingQueue.count())

      loadOnIteration = sc.parallelize(mainLoadingQueue.take(usersPerIteration))
      println("LOI.size = " + loadOnIteration.count())

      mainLoadingQueue = mainLoadingQueue.subtract(loadOnIteration)
      println("MLQ.size = " + mainLoadingQueue.count())

      // update downloading queue

      currentUsers.foreach(u =>  ElasticSearchHelper.addUser(u))
      if(storageDirectory != null)
        currentUsers.foreach(u => VkUser.storeUser(u, storageDirectory))

      println((if (i==0) "1" else usersPerIteration) + " more users added to index")
    }

    println("success!")
  }
}
