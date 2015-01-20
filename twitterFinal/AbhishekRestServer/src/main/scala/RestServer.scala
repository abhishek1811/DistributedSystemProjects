package main.scala

import spray.routing.SimpleRoutingApp
import spray.routing._
import akka.actor._
import scala.concurrent.duration._
import akka.util._
import akka.pattern._
import akka.pattern.AskTimeoutException
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.Directive.pimpApply
import spray.routing.directives.ParamDefMagnet.apply
import twitter.messages.NormalMessages._

object RestServer extends App with SimpleRoutingApp {

  import system.dispatcher

  implicit val system = ActorSystem("TwitterRest")
  implicit val timeout = Timeout(2.seconds)
  if (args.length != 2) {
      println("-------------------Invalid Input-----------------\nThe command line input for this program is as follow:\n scala RestServerIP ServerIP")
      System.exit(0)
  }
  val ipAddress = args(0)
  var startServeraddress= args(1)
  val serveraddress = "akka.tcp://Twitter@" + startServeraddress + ":8700/user/servermaster/ServerWorker"
  val server = system.actorFor(serveraddress)
  println("Server Worker: " + server)
  startServer(interface = ipAddress, port = 2559) {
    get {
      path("server") {
        complete {
          startServeraddress
        }
      }
    } ~
    post {
      path("tweeting") {
        parameter("userid".as[Int], "tweet", "tweetid") { (userid, tweet, tweetid) =>
          
          server ! tweeting(userid.toInt, tweet, tweetid)
          println("Tweet....")
          complete {
            "Tweet Posted"
          }

        }
      }
    } ~
      post {
        path("retweet") {
          parameter("tweetid", "userid".as[Int]) { (tweetid, userid) =>
            server ! retweeting(tweetid, userid)
            println("Retweet....")
            complete {
              "Retweet Posted"
            }
          }
        }
      } ~
      get {
        path("tweets") {
          parameter("userid".as[Int]) { (userid) =>
            println("GetTweet..")
            complete {
              (server ? returnTweets(userid)).recover {
                case ex: AskTimeoutException => {
                  "Tweeting Request Failed"
                }
              }
                .mapTo[String]
            }

          }
        }
      } ~
      get {
        path("followers") {
          parameter("userid".as[Int]) { (userid) =>
            println("GetFollowers....")
            complete {
              (server ? getFollowers(userid)).recover {
                case ex: AskTimeoutException => {
                  "Tweeting Request Failed"
                }
              }
                .mapTo[String]
            }

          }
        }
      } ~
      get {
        path("mentions") {
          parameter("userid".as[Int]) { (userid) =>
            println("GetMentions....")
            complete {
              (server ? returnMentions(userid)).recover {
                case ex: AskTimeoutException => {
                  "Tweeting Request Failed"
                }
              }
                .mapTo[String]
            }

          }
        }
      }

  }
}
