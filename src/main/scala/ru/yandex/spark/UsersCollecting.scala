package ru.yandex.spark

import Spark.sc

object UsersCollecting {


  def main(args: Array[String]): Unit = {

    val storageDirectory =
    if(args.length != 0)
      args(0)
    else
      "D:\\tmp\\downloading\\"


    val usersToLoad = sc.parallelize(List("1"))
    val user = VkUser.addExtraInformation(User("1"))

    val loadedUser = sc.parallelize(List(user))

    loadedUser.saveAsObjectFile(storageDirectory + 1)
    println("success!")
  }
}
