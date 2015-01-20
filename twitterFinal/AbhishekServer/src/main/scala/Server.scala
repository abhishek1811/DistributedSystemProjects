package main.scala

import akka.actor._
import scala.util.Random
import java.util.ArrayList
import java.util.Collections
import org.json4s.ShortTypeHints
import scala.concurrent.duration._
import scala.util.control.Breaks._
import java.util.concurrent.TimeUnit
import akka.routing.RoundRobinRouter
import org.json4s.native.Serialization
import scala.collection.mutable.HashMap
import com.typesafe.config.ConfigFactory
import org.json4s.native.Serialization._
import twitter.messages.NormalMessages._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.SynchronizedBuffer
import scala.concurrent.ExecutionContext.Implicits.global

object Server {
  
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("-------------------Invalid Input-----------------\nThe command line input for this program is as follow:\n scala Project4  scalingfactor  ClientMachine TimeofRunning");
      System.exit(0)
    }

    val sf = args(0).toInt
    val maxclients = args(1).toInt
    Global.Runningtime = args(2).toInt
    val s_actors = ((Runtime.getRuntime().availableProcessors()) * 1.5).toInt
    val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=8700").withFallback(ConfigFactory.load()).resolve
    var system = ActorSystem("Twitter", config)
    System.setProperty("java.net.preferIPv4Stack", "true")
    var servermaster = system.actorOf(Props(new ServerMaster(maxclients, s_actors)), name = "servermaster")
    println(" Scaling Factor: " + sf + "\n Max Server Actors: " + s_actors + "\n Max Client Machine: " + maxclients)
    println(servermaster.path)
    servermaster ! "Initiate"

  }
  class ServerMaster(maxclients: Int, s_actors: Int) extends Actor {

    var endid = 0
    var tempst = 0
    var tempend = 0
    var waiting = 0
    var startid = 0
    var totalusers = 0
    Global.maxclients = maxclients
    var clientref: Array[ActorRef] = _
    val serverworker = context.actorOf(Props[ServerWorker].withRouter(RoundRobinRouter(s_actors)), name = "ServerWorker")

    def printme(a: Array[ActorRef]) = {

      for (i <- 0 to a.length - 1)
        println(a(i))
      println()
    }

    def printRelations() = {

      for (i <- 0 to Global.user.size - 1) {

        println("--------------------------------------------------------------------")
        println("ID:" + i + "\nFollowers: " + Global.user(i).followers.size + "\nFollowing: " + Global.user(i).following.size)
        println()
        println("-------------------------------------------------------------------")
      }
      println("Total Following: " + Global.sum + "Total Relations: " + Global.relationcounter)
    }

    def printTweets() = {

      for (i <- 0 to Global.user.size - 1) {
        println("userid " + i + "\n---------------------------")
        println(Global.user(i).tweets.size)
        println("\n---------------------------")
      }
    }

    def assignStats(): Array[Double] = {

      var percent = Array(10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 96, 97, 98, 99, 99.9)
      var key: Array[Double] = new Array[Double](16)
      for (i <- 0 to 14) {
        key(i) = (percent(i) * Global.totalusers * .01)
      }
      return key
    }

    def FollowersStats(): Array[Double] = {

      var percent = Array(2.09, 12.82, 23.56, 32.73, 40.18, 46.17, 51.02, 55.03, 58.40, 61.29, 63.77, 65.97, 67.90, 69.62, 71.18, 72.57, 73.83, 74.98, 76.03, 93.68, 96.43, 97.47, 98.03, 98.17, 98.90)
      var key: Array[Double] = new Array[Double](25)
      for (i <- 0 to 24) {
        key(i) = (percent(i) * Global.totalusers * .01)
      }
      return key

    }
    def FollowingStats(): Array[Double] = {

      var percent = Array(7.01, 18.09, 24.29, 29.14, 33.27, 36.91, 40.12, 43.00, 45.58, 47.92, 50.06, 52.01, 53.81, 55.47, 57.00, 58.43, 59.77, 61.05, 62.28, 92.46, 95.99, 97.19, 97.79, 98.00, 99.00)
      var key: Array[Double] = new Array[Double](25)
      for (i <- 0 to 24) {
        key(i) = percent(i) * Global.totalusers * .01
      }
      return key
    }

    def makeFollowing(): ArrayBuffer[Int] = {
      var stats = FollowingStats()
      var a = ArrayBuffer.empty[Int]
      var nooffollowing = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 100, 200, 300, 400, 500, 1000)

      var count = 0
      var from = 0
      var till = nooffollowing(count)
      var randomfound = 0
      var r = Random
      var totusers = Global.totalusers

      for (i <- 0 to Global.user.size - 1) {

        if (i > stats(count)) {

          if (count >= 23)
            till = 50000
          else if (count < 23) {
            from = till
            count = count + 1
            till = nooffollowing(count)
          }
        }

        if (i <= stats(count) && (count < 19)) {

          a += nooffollowing(count)
        }

        if (i <= stats(count) && (count >= 19) || (till == 50000)) {

          val range = from to till
          var rand = range(r.nextInt(range.length))
          a += rand
        }

      }

      return a
    }

    def makeFollowers(): ArrayBuffer[Int] = {
      var stats = FollowersStats()
      var a = ArrayBuffer.empty[Int]
      var nooffollowing = Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 100, 200, 300, 400, 500, 1000)

      var count = 0
      var from = 0
      var till = nooffollowing(count)
      var randomfound = 0
      var r = Random
      var totusers = Global.totalusers

      for (i <- 0 to Global.user.size - 1) {

        if (i > stats(count)) {

          if (count >= 23)
            till = 50000
          else if (count < 23) {
            from = till
            count = count + 1
            till = nooffollowing(count)
          }
        }

        if (i <= stats(count) && (count < 19)) {

          a += nooffollowing(count)
        }

        if (i <= stats(count) && (count >= 19) || (till == 50000)) {

          val range = from to till
          var rand = range(r.nextInt(range.length))
          a += rand

        }

      }

      return a
    }
    def replacerelation() = {

      var nooffollowers = makeFollowers()
      var nooffollowing = makeFollowing()
      Global.sum = nooffollowing.sum

      for (i <- 0 until Global.user.size ) {
        var k = -1
        while ((nooffollowing(i) > 0) && (k < Global.user.size - 1)) {
          k += 1
          if ((nooffollowers(k) > 0) && (k < Global.user.size - 1) && (k != i)) {
            Global.relationcounter = Global.relationcounter + 1
            Global.user(i).following += Global.user(k).userid
            Global.user(k).followers += Global.user(i).userid
            nooffollowing(i) -= 1
            nooffollowers(k) -= 1
          }
        }
      }
    }

    def receive = {

      case "Initiate" => {
        println("ServerMaster waiting for Clients....")
        Global.clientref = new Array[ActorRef](maxclients)
      }

      case create(maxID) => {

        sender ! getDuration(Global.Runningtime)
        serverworker ! BuildStats(maxID, sender)

      }

      case getFrequency(frequency) => {
        Global.frequency = frequency
      }

      case MakeRelations(maxID) => {

        if (Global.waiting == Global.maxclients) {

          println("Server Master is now creating the relations (Followers/Following)")
          println("Sending to ....")
          printme(Global.clientref)
          replacerelation()

          for (i <- 0 to Global.clientref.length - 1 ) {
            println("Server Master is now sending the ids")
            Global.tempst = Global.tempend
            Global.tempend += maxID
            Global.clientref(i) ! AssignIDS(Global.tempst, Global.tempend)
          }

          context.system.scheduler.schedule(Duration.create(5, TimeUnit.SECONDS),
            Duration.create(1, TimeUnit.SECONDS)) {

              println("Messages processed per sec: " + Global.messageprocessed)
              Global.messageprocessed = 0
            }

          context.system.scheduler.scheduleOnce(Global.Runningtime.seconds) {

            context.system.shutdown
            for (i <- 0 to maxclients - 1) {
              Global.clientref(i) ! "terminate"
            }
          }

        }
      }

    }
  }

  class ServerWorker extends Actor {
   
    private implicit val formats = Serialization.formats(ShortTypeHints(List(classOf[String])))
    
    def myWritePretty(array:Array[String]):String ={
      var list = new ArrayList[String]()
      for(i<-0 until array.length ){
        if(array(i)!=null)
            list.add(array(i))
      }
      writePretty(list)
    }
    def receive = {

      case getFollowers(clientId: Int) => {

        sender ! writePretty(Global.user(clientId).followers)
        Global.messageprocessed = Global.messageprocessed + 1
      }
      case returnTweets(userid) => {
        sender ! myWritePretty(Global.user(userid).tweets)
        Global.messageprocessed = Global.messageprocessed + 1
      }
      case returnMentions(userid) => {
        sender ! myWritePretty(Global.user(userid).mentions)
        Global.messageprocessed = Global.messageprocessed + 1
      }
      case retweeting(tweetId, myuserid) => {

        var message = Global.tweetcollection(tweetId)
        Global.user(myuserid).storemymessage(message)
        var x = 0
        while (x != Global.user(myuserid).followers.size) {
          Global.user(Global.user(myuserid).followers(x)).storemymessage(message)
          x += 1

        }
        
        Global.messageprocessed = Global.messageprocessed + 1
      }
      case tweeting(myuserid, message, tweetId) => {
    	 
        if (message.startsWith("@")) {
          Global.tweetcollection += (tweetId -> message)
          var index = message.indexOf(":")
          var useridfound = message.substring(1, index).toInt
          Global.user(useridfound).storemymention(message)
          for (i <- 0 until Global.user(myuserid).followers.size) {
            for (j <- 0 until Global.user(useridfound).followers.size ) {
              if (Global.user(myuserid).followers(i) == Global.user(useridfound).followers(j)) {
                Global.user(i).storemymessage(message)
              }
            }
          }

        } 
        else {
          Global.tweetcollection += (tweetId -> message)
          Global.user(myuserid).storemymessage(message)
          var x = 0
          while (x != Global.user(myuserid).followers.size) {
            Global.user(Global.user(myuserid).followers(x)).storemymessage(message)
            x += 1
          }
        }

        Global.messageprocessed = Global.messageprocessed + 1
      }

      case BuildStats(maxID, senderaddress) => {

        Global.totalusers = Global.maxclients * maxID
        if (Global.waiting <= Global.maxclients) {

          Global.endid += maxID
          Global.waiting += 1
          println("Creating Userid's for Client: " + Global.waiting)
          Global.clientref(Global.waiting - 1) = senderaddress

          while (Global.startid != Global.endid) {
            Global.user += (Global.startid -> new Users(Global.startid))
            Global.startid += 1
          }

        }

        if (Global.waiting == Global.maxclients) {
          println("Now making relations")
          sender ! MakeRelations(maxID)
        }

      }
    }
  }

  class Users(id: Int) {

    var messagecounter = -1
    var mentioncounter = -1
    var userid: Int = id
    var tweets: Array[String] = new Array[String](100)
    var followers = new ArrayBuffer[Integer]() with SynchronizedBuffer[Integer]
    var following = new ArrayBuffer[Integer]() with SynchronizedBuffer[Integer]
    var mentions = new Array[String](100)


    def storemymessage(message: String) = {
      if (messagecounter == 99)
        messagecounter = -1
      messagecounter = messagecounter + 1
      tweets(messagecounter) = message

    }

    def storemymention(message: String) = {
      if (mentioncounter == 99)
        mentioncounter = -1
      mentioncounter = mentioncounter + 1
      mentions(mentioncounter) = message

    }

  }

  object Global {

    var sum = 0
    var endid = 0
    var tempst = 0
    var tempend = 0
    var waiting = 0
    var startid = 0
    var counter = 0
    var frequency = 0
    var totalusers = 0
    var maxclients = 0
    var Runningtime = 0
    var relationcounter = 0
    var messageprocessed = 0
    var clientref: Array[ActorRef] = _
    var user = HashMap.empty[Int, Users]
    var tweetcollection = new HashMap[String, String]

  }

}