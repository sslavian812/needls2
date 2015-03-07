package ru.yandex.spark

import Spark.sc
import breeze.optimize.MaxIterations
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets


object UsersCollecting {


  def main(args: Array[String]): Unit = {

    val storageDirectory =
    if(args.length != 0)
      args(0)
    else
      "D:\\tmp\\downloading"

    val iterations : Int=
    if(args.length == 2)
      args(1).toInt
    else
      2

    var idsToLoad = sc.parallelize(List("1", "2"))
    var loadedIds = sc.parallelize(List[String]())

    for( i <- 1 until iterations) {
      val currentUsers = idsToLoad.map(u => VkUser.addExtraInformation(User(u)))
      // load Users

      loadedIds = loadedIds.union(currentUsers.map(u => u.id))
      val friends = currentUsers.map(u => u.friends).reduce((l1, l2) => List.concat(l1, l2)).distinct
      idsToLoad = sc.parallelize(friends.map(x => "" + x)).subtract(loadedIds)
      // update downloading queue
      
      currentUsers.foreach(u => storeUser(u, storageDirectory))
      // storing on disc
    }

    println("success!")
  }

  def storeUser(user: User, path : String): Unit = {
    Files.write(Paths.get(path + "\\" + user.id + ".txt" ), pretty(VkUser.getJsonRepresentation(user)).getBytes(StandardCharsets.UTF_8))
  }
}
