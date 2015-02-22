package ru.yandex.spark

import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._


case class User(
                 id: String,
                 generalInformation: GeneralInformation = GeneralInformation(),
                 groupInformation: GroupInformation = GroupInformation(),
                 sub_user_count: Int = Int.MinValue,
                 followers_count: Int = Int.MinValue,
                 wall_count: Int = Int.MinValue,
                 albums_count: Int = Int.MinValue,
                 friends: List[Long] = List.empty,
                 subscriptions: List[Long] = List.empty
)

case class GroupInformation(
                             sub_groups_count: Int = Int.MinValue,
                             groups: List[Long] = List.empty
                             )

case class GeneralInformation(
                               idLong: Long = 0,
                               yearOfBirth: Int = Int.MinValue,
                               monthOfBirth: Int = Int.MinValue,
                               sex: Int = Int.MinValue,
                               uidIsChanged: Int = 1,
                               city: Int = Int.MinValue,
                               has_mobile: Int = Int.MinValue,
                               can_see_all_posts: Int = Int.MinValue,
                               twitter: Int = Int.MinValue,
                               can_see_audio: Int = Int.MinValue,
                               site: Int = Int.MinValue,
                               occupation: Int = Int.MinValue,
                               university: Int = Int.MinValue,
                               graduation_year: Int = Int.MinValue,
                               education_form: Int = Int.MinValue,
                               relation: Int = Int.MinValue,
                               country: Int = Int.MinValue
                               )

object VkUser {

  val URL_BEGINNING = "https://api.vk.com/method/"
  implicit val formats = DefaultFormats

  def getUrlForMethod(method: String) = {
    URL_BEGINNING + method + "?"
  }

  def addParameter(parametr: String, value: String) = {
    parametr + "=" + value + "&"
  }

  def addExtraInformation(user: User) = {
    println("Downloading user " + user.id)
    var changedUser = user
    changedUser = tr(changedUser, { user => addGeneralInformation(user)})
    changedUser = tr(changedUser, { user => addSubscribtionInformation(user)})
    changedUser = tr(changedUser, { user => addFollowersInformation(user)})
    changedUser = tr(changedUser, { user => addWallInformation(user)})
    changedUser = tr(changedUser, { user => addPhotoInformation(user)})
    changedUser = tr(changedUser, { user => addFriendsInformation(user)})
    changedUser
  }


  def addSubscribtionInformation(user: User) = {
    val s = Source.fromURL(getUrlForMethod("users.getSubscriptions")
      + addParameter("user_id", user.generalInformation.idLong.toString)).mkString
    val parsed: JValue = parse(s)

    var parsedUser = tr(user, {
      user =>
        user.copy(sub_user_count = (
          (parsed \ "response" \ "users" \ "count").extract[Int]))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(groupInformation = user.groupInformation.copy(sub_groups_count = (
          (parsed \ "response" \ "groups" \ "count").extract[Int])))
    })


    parsedUser = tr(parsedUser, {
      user =>
        user.copy(groupInformation = user.groupInformation.copy(groups = (
          (parsed \ "response" \ "groups" \ "items").extract[List[Long]])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(subscriptions =
          (parsed \ "response" \ "groups" \ "users").extract[List[Long]])
    })

    parsedUser
  }

  def addFollowersInformation(user: User) = {
    val s = Source.fromURL(getUrlForMethod("users.getFollowers")
      + addParameter("user_id", user.generalInformation.idLong.toString)).mkString
    val parsed: JValue = parse(s)

    val parsedUser = tr(user, {
      user =>
        user.copy(followers_count = (
          (parsed \ "response" \ "count").extract[Int]))
    })

    parsedUser
  }

  def addFriendsInformation(user: User) = {
    val s = Source.fromURL(getUrlForMethod("friends.get")
      + addParameter("user_id", user.generalInformation.idLong.toString)).mkString
    val parsed: JValue = parse(s)
    val parsedUser = tr(user, {
      user =>
        user.copy(friends = (
          (parsed \ "response").extract[List[Long]]))
    })

    parsedUser
  }

  def addWallInformation(user: User) = {
    val s = Source.fromURL(getUrlForMethod("wall.get")
      + addParameter("owner_id", user.generalInformation.idLong.toString)).mkString
    val parsed: JValue = parse(s)

    val parsedUser = tr(user, {
      user =>
        user.copy(wall_count = (
          (parsed \ "response" \ "count").extract[Int]))
    })

    parsedUser
  }

  def addPhotoInformation(user: User) = {
    val s = Source.fromURL(getUrlForMethod("photos.getAlbums")
      + addParameter("owner_id", user.generalInformation.idLong.toString)).mkString
    val parsed: JValue = parse(s)

    val parsedUser = tr(user, {
      user =>
        user.copy(wall_count = (
          (parsed \ "response" \ "count").extract[Int]))
    })

    parsedUser
  }

  def addGeneralInformation(user: User) = {
    val whatToReturn = "id,sex,bdate,city,country,photo_50,photo_100,photo_200_orig,photo_200,photo_400_orig,photo_max,photo_max_orig,photo_id,online,online_mobile,domain,has_mobile,contacts,connections,site,education,universities,schools,can_post,can_see_all_posts,can_see_audio,can_write_private_message,status,last_seen,relation,relatives,counters,screen_name,maiden_name,timezone,occupation,activities,interests,music,movies,tv,books,games,about,quotes"
    val s = Source.fromURL(
      getUrlForMethod("users.get") +
        addParameter("fields", whatToReturn) +
        addParameter("user_ids", user.id)).mkString
    var parsedUser = user

    implicit val formats = DefaultFormats

    val parsed: JValue = parse(s)
    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(yearOfBirth = (
          (parsed \ "response" \ "bdate").extract[String]).split('.')(2).toInt))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(idLong =
          (parsed \ "response" \ "uid").extract[Long]))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(monthOfBirth = (
          (parsed \ "response" \ "bdate").extract[String]).split('.')(1).toInt))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(sex = (
          (parsed \ "response" \ "sex").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        val screen_name = (parsed \ "response" \ "screen_name").extract[String]
        if (screen_name.startsWith("id"))
          user.copy(generalInformation = user.generalInformation.copy(uidIsChanged = 0))
        else user.copy(generalInformation = user.generalInformation.copy(uidIsChanged = 1))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(city = (
          (parsed \ "response" \ "city").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(country = (
          (parsed \ "response" \ "country").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(has_mobile = (
          (parsed \ "response" \ "has_mobile").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(can_see_all_posts = (
          (parsed \ "response" \ "can_see_all_posts").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(can_see_audio = (
          (parsed \ "response" \ "can_see_audio").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(twitter = (
          if ((parsed \ "response" \ "twitter").extract[String].size > 2) 1 else 0)))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(site = (
          if ((parsed \ "response" \ "site").extract[String].size > 2) 1 else 0)))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(occupation = (
          if ((parsed \ "response" \ "occupation" \ "type").extract[String].equals("work")) 1 else 0)))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(university = (
          (parsed \ "response" \ "university").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(graduation_year = (
          (parsed \ "response" \ "graduation").extract[Int])))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(education_form = (
          if ((parsed \ "response" \ "education_form").extract[String].equals("Дневное отделение")) 1 else 0)))
    })

    parsedUser = tr(parsedUser, {
      user =>
        user.copy(generalInformation = user.generalInformation.copy(relation = (
          (parsed \ "response" \ "relation").extract[Int])))
    })

    parsedUser
  }

  def tr(user: User, f: (User) => User): User = {
    try {
      f.apply(user)
    } catch {
      case e: Exception => {
        //e.printStackTrace()
        user
      }
    }
  }
}
