package ru.yandex.spark

import Spark.sc
import org.json4s._
import org.json4s.jackson.JsonMethods._


/**
 * usage: path iterations start_user_id
 */
object UsersCollecting {

  implicit val formats = DefaultFormats

  def main(args: Array[String]): Unit = {

    val storageDirectory =
    if(args.length >= 1)
      args(0)
    else
      "D:\\tmp\\downloading"

    val iterations : Int=
    if(args.length >= 2)
      args(1).toInt
    else
      1

    val startId : String =
    if(args.length >= 3)
      args(2)
    else
      "1"

    var idsToLoad = sc.parallelize(List(startId))
    var loadedIds = sc.parallelize(List[String]())

    for( i <- 0 until iterations) {

        val currentUsers = idsToLoad.map(u => VkUser.addExtraInformation(User(u)))
        // load Users

        loadedIds = loadedIds.union(currentUsers.map(u => u.id))
        val friends = currentUsers.flatMap(u => u.friends).distinct()
        idsToLoad = friends.map(x => "" + x).subtract(loadedIds)
        // update downloading queue

        currentUsers.foreach(u => VkUser.storeUser(u, storageDirectory))
        // storing on disc
    }

    println("success!")
  }
}
