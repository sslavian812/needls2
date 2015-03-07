package ru.yandex.spark

import Spark.sc
import org.json4s._
import org.json4s.jackson.JsonMethods._


object UsersCollecting {


  def main(args: Array[String]): Unit = {

    val storageDirectory =
    if(args.length != 0)
      args(0)
    else
      "D:\\tmp\\downloading\\"


    var idsToLoad = sc.parallelize(List("1", "2"))
    var loadedIds = sc.parallelize(List[String]())


    for( i <- 1 until 3){
      val currentUsers = idsToLoad.map(u => VkUser.addExtraInformation(User(u)))
          // load Users

      loadedIds = loadedIds.union(currentUsers.map(u => u.id))
      val friends = currentUsers.map(u => u.friends).reduce((l1, l2) => List.concat(l1, l2)).distinct
      idsToLoad = sc.parallelize(friends.map(x => ""+x)).subtract(loadedIds)
          // update downloading queue

      val currentUsersAsJson = currentUsers.map(u => pretty(VkUser.getJsonRepresentation(u)))
      currentUsersAsJson.saveAsTextFile(storageDirectory+i)
          // storing on disc

    }

    println("success!")
  }
}
