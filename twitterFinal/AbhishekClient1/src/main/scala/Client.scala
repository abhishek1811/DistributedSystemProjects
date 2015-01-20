

package main.scala

import akka.actor._
import scala.util.Random
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import java.util.ArrayList
import java.util.Collections
import spray.http._
import spray.client.pipelining._
import akka.actor.ActorSystem

object Client {

  implicit val system = ActorSystem("TwitterClient")
  import system.dispatcher
  import twitter.messages.NormalMessages._

  val pipeline = sendReceive
  var ServerAddress = "http://"

  def IPaddressCheck(IP: String) = {
    if (IP == " ") {
      println("IP Address is not valid,Sorry try Again...")
      System.exit(0)
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("-------------------Invalid Input-----------------\nThe command line input for this program is as follow:\n scala Client Ipaddress(Server) ScalingFactor Option(Event/Regular) mainserverip")
      System.exit(0)
    }

    var sf = if (args.length < 2) 0 else args(1).toInt
    Global.clients = (1000000 * sf) / 100
    Global.option = args(2)
    //Global.mainserver = args(3)
    var IPaddress = if ((args.length > 1) && (args(0).containsSlice("."))) args(0) else " "
    IPaddressCheck(IPaddress)
    ServerAddress += IPaddress + ":2559/"
    println("I am here")
    val result = pipeline(Get(ServerAddress + "server"))
    var address = ""
    result.foreach {
      response =>
        println(response.entity.asString)
        address = response.entity.asString

        println(" IPaddress of Server: " + IPaddress + "\n Client Users: " + Global.clients)
        System.setProperty("java.net.preferIPv4Stack", "true")
        var clientmaster = system.actorOf(Props(new ClientMaster(Global.clients, address)), name = "clientmaster")
        clientmaster ! "requestnow"
    }

  }

  class ClientMaster(clients: Int, IPaddress: String) extends Actor {
    Global.IP = IPaddress
    val servermaster = context.actorFor("akka.tcp://Twitter@" + Global.IP+ ":8700/user/servermaster")
    println("Master reference defined " + IPaddress)
    var myclients: Array[ActorRef] = new Array[ActorRef](clients)
    var start: Long = 0
    var end: Long = 0

    def receive = {

      case "requestnow" => {

        println(self.path + " In requestnow")
        servermaster ! create(clients)
      }

      case AssignIDS(tempst, tempend) => {

        start = System.nanoTime();
        var absolutetime = System.currentTimeMillis + 3500
        for (i <- 0 to clients - 1) {
          myclients(i) = system.actorOf(Props(new ClientUser(tempst + i)), name = "clientuser" + (tempst + i))
          //myclients(i) ! Ready(Global.option)
          myclients(i) ! schedule(absolutetime)
        }

      }
      case getDuration(timing) => {

        Global.Runningtime = timing
        println("Runningtime: " + Global.Runningtime)
        var avgtweetspersec = 5787
        val Avgtweetsperuserforduration = ((5787 * Global.Runningtime) / clients).toInt
        Global.frequency = (Global.Runningtime / Avgtweetsperuserforduration).toInt
        if (Global.option == "Regular")
          sender ! getFrequency(Global.frequency)
        else
          sender ! getFrequency(1)
      }
      case "terminate" => {
        context.system.shutdown
      }

    }
  }
  class ClientUser(myid: Int) extends Actor {

    var userid: Int = myid
    var tweetbank1 = new Array[String](100)
    var result = pipeline(Get(ServerAddress + "server"))

    def generateTweetId(): String = {
      Global.tweetNumber =Global.tweetNumber+1
      var id = "" + System.currentTimeMillis()
      Global.tweetIds.add(id)
      id
    }

    def getRandomTweetID(): String = {
      Global.tweetIds.get(Random.nextInt((Global.tweetIds.size/10).toInt))
    }
    def twitterAPI()={

      var r = Random
      var indexofmessage = r.nextInt(100)
      if (indexofmessage % 5 == 0) {
        result = pipeline(Get(ServerAddress + "followers?userid=" + userid))
        //printResult
      } else if (indexofmessage % 7 == 0) {
        result = pipeline(Get(ServerAddress + "tweets?userid=" + userid))
        //printResult
      } else if (indexofmessage == 31){
        pipeline(Post(ServerAddress + "retweet?tweetid=" + getRandomTweetID + "&userid=" + userid))
         
       }
      else if (indexofmessage % 13 == 0) {
        var sendingTweetId = generateTweetId()
        var mentionuserid = r.nextInt(Global.clients)
        pipeline(Post(ServerAddress + "tweeting?userid=" + userid + "&tweet=" + "@" + mentionuserid + ":" + Global.tweetbank(indexofmessage) + "&tweetid=" + sendingTweetId))
      } else if (indexofmessage % 19 == 0) {
        result = pipeline(Get(ServerAddress + "mentions?userid=" + userid))
        //printResult
      } else {
        var sendingTweetId = generateTweetId()
        pipeline(Post(ServerAddress + "tweeting?userid=" + userid + "&tweet=" + Global.tweetbank(indexofmessage) + "&tweetid=" + sendingTweetId))
      }

    }
    def receive = {

      case Ready(option) => {

        if (option == "Event") {
          context.system.scheduler.schedule(Duration.create(600, TimeUnit.MILLISECONDS),
            Duration.create(1, TimeUnit.SECONDS)) {
            twitterAPI
            }
        } 
        else if (option == "Regular") {
          context.system.scheduler.schedule(Duration.create(600, TimeUnit.MILLISECONDS),
            Duration.create(Global.frequency, TimeUnit.SECONDS)) {
              twitterAPI
            }
        }
      }
      
      case schedule(absolutetime) => {

        var time = absolutetime - System.currentTimeMillis()
        context.system.scheduler.scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS), self, Ready(Global.option))
      }
    }
    def printResult() {
      result.foreach {
        response =>
          println(s"Request completed with status ${response.status} and content:\n${response.entity.asString}")
      }
    }
  }

  object Global {

    var i = 0
    var IP = ""
    var option = ""
    var clients = 0
    var frequency = 0
    var tweetNumber = 0
    var Runningtime = 0
    var tweetcounter = 0
    var mainserver =""
    var tweetbank = new Array[String](100)
    var tweetIds = Collections.synchronizedList(new ArrayList[String]())
    val filename = "/Users/Abhishek/Desktop/twitterFinal/AbhishekClient1/myfile.txt"
    for (line <- scala.io.Source.fromFile(filename).getLines()) {
      tweetbank(i) = line
      i += 1
    }

  }

}
