//learning process
package server
import akka.actor._
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import akka.event.Logging
import akka.event.LoggingAdapter
import common._

// Messages used for communication between Server and Client.
sealed trait BitcoinMessages
case object Start extends BitcoinMessages
case object Send extends BitcoinMessages
case class msgint(msg:Int) extends BitcoinMessages
case class getWork(noOfWorkers:Int,from:Int,end:Int,zeroes:Int) extends BitcoinMessages
case class exchange(msg: Int) extends BitcoinMessages 
case class Message(msg: String) extends BitcoinMessages
case class collect(x:String) extends BitcoinMessages
case class Over (terminate : Int) extends BitcoinMessages
case object FindBitcoin extends BitcoinMessages 
case class Done (shaDone : Int) extends BitcoinMessages
case class Work (from : Int , end : Int , leadingZeroes : Int) extends BitcoinMessages

/* 
 *  Main object of the the code which takes arguments for user and initiate the mining of bitcoins.
 *  As command line argument,it takes the number of zeroes that should be in the required bitcoins. 
 */
 object Server extends App {

  // Global variables contains number of bitcoins mined throughout the mining. Increases when new bitcoin found.
  var coincounter = 0
  var numberOfZeros = 1
  var bitcoinRange = 10000000
  // argument takes in command line input. If its greater than or lesser than one then its empty.
  val argument = if(args.length == 1) args(0) else " "
  /*
   * Checks whether whether the command line argument is valid or not.
   * Also as per defined condition sets the value for numberOfZeros.
   */
   def assign(argument:String)={
    //If argument is empty, then exit with error message .
    if(argument == " ")
    {
      println("*********Invalid Argument**********" +"\n Input should be of type : \n scala Project1 leadingZeroes   or\n scala Project1 IPAddress" +"\n ***********************************")
      System.exit(1)
    }
    //If it contains " . " which depicts it might be IP address.Thus assign numberofZeroes........ Error not handles " ."
    else if(argument.containsSlice(".")){
      println(argument)
    }
    // take the command line argument.
    else
    numberOfZeros = argument.toInt
  }

  assign(argument)
  // defining the actor system and actors used in the code.
  val system = ActorSystem("ServerSystem")
  //val terminators = system.actorOf(Props[Terminator], name = "terminators")
  val s_actors = ((Runtime.getRuntime().availableProcessors()) * 1.5).toInt
  val master = system.actorOf(Props(new Master(s_actors,bitcoinRange,numberOfZeros)),name = "master") 
  println(master.path)
  master ! FindBitcoin
  
  /*
   *  Worker class is used to do all the mining of bitcoins when master gives the work. Actors of this  
   *  class are defined in Master class. Each worker actor is assigned a chunk to mine required Bitcoins.
   */
   class Worker extends Actor {

    // generates pattern according to numberOfZeros and returns the pattern created.
    def createPattern(max:Int)={  
      var pattern = ""
      for (j <- 1 to max) {
        pattern = pattern + "0"
      }
      pattern
    }
    /*
     *  This function takes in the chunk with some range defined through arguments from ,end and 
     *  also numberOfZeros used to find required Bitcoins. The function creates sha 256 hash code of 
     *  each string formed in the chunk and checks whether it is required Bitcoin or not. Updates number
     *  of bitcoin mined in coincounter variable. Return message Done which is defined in Master's receive.
     */
     def shaHasher(from: Int, end: Int, numberOfZeros: Int) =
     {  
      var pattern: String = createPattern(numberOfZeros)
      var hashstrings: String = ""
      // instantiates the object of sha-256 to sha
      val sha = MessageDigest.getInstance("sha-256")
      // generate all hash strings b/w a parametrized set.
      for(i <- from to end - 1) {
	      // generates all the hex string, foldleft is curried function used on sha list
	      var hexstring = sha.digest(("n.sadhwani" + i.toString()).getBytes).foldLeft("")((s: String, b: Byte) => s +
         Character.forDigit((b & 0xf0) >> 4, 16) + Character.forDigit(b & 0x0f, 16))
	      // select all the hash strings which starts with required pattern and update coincounter as it.
	      if(hexstring.startsWith(pattern)) {
          hashstrings += "n.sadhwani" + i + " " + hexstring + "\n"
          coincounter +=1
          println(hashstrings)
        }
      } 
      Done(coincounter)
    }
    def receive = {
      /*
       * Message that accepts assigned chunk to for each worker and reply sender with information of number of chunks
       * processed and Bitcoins mined through function shaHasher.
       */
       case Work(from,end,leadingZeroes) ⇒
       sender ! shaHasher(from,end,leadingZeroes)
     }

   }

  /*
   *  Master actor assign chunks to its worker and remote client till all chunks to be processed are over.
   *  If remote client contacts the master for work then master assign some chunks to client otherwise master
   *  assigns all chunks to its worker.
   */
   class Master(noOfWorkers:Int, TotalElements:Int, leadingZeroes:Int) extends Actor 
   {
    // client variables used to define chunk for clients and also bitcoins processed by client.
    var clientCount = 1
    var clientBitcoins = 0 
    var clientBuffer = 1000000
    var clientChunkEnd = 11000000
    var clientChunkFrom = 10000000
    // number of chunks is calculated by dividing totalset into chunks of size 10000.
    var numberOfChunks = (TotalElements/10000)
    // counts the number of chunks processed
    var chunkCounter = 1
    val clientworker = ((Runtime.getRuntime().availableProcessors()) * 1.2).toInt  
    val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(noOfWorkers)), name = "workerRouter")

    def receive = {
    	// sends information respective clients.
      case "Ready" => 	
      println ("I am sending")
       //sender ! getWork(clientworker,clientChunkFrom,clientChunkEnd,leadingZeroes)
       sender !  clientChunkFrom
       sender !  clientChunkEnd
       sender !  leadingZeroes
       sender !  clientworker 
       clientChunkFrom = clientChunkFrom + clientBuffer
       clientChunkEnd = clientChunkEnd + clientBuffer
      /* 
       * using RoundRobin Algorithm sending chunks of size 10000 to master's worker till all chunks
       * are processed
       */
       case FindBitcoin =>
       for (i <-1 until numberOfChunks)
       {
         workerRouter ! Work((i-1) * 10000, i * 10000, numberOfZeros)	
       }
	    /*
       * Checks if all chunks are processed or not. Once all chunks are processed it prints
       * the total number of bitcoins processed by Server and Client.At end terminates the 
       * server with system shutdown.
       */ 
       case Done(shaDone) => 
       chunkCounter += 1
       if(chunkCounter == numberOfChunks){
         println("Total Bit Coins Mined :" + (shaDone+clientBitcoins) + "\n Terminating..........")
         context.system.shutdown()
  		    //context.stop(self)		
      }
      // print the strings accepted from client
      case msg: String => 
      println("*****---------Bitcoins received from client "+clientCount+"-----------------------*********")
      clientCount = clientCount + 1
      println(msg.toString) 	
      println("*********------------------------------------------------------------------*****************")
      // prints the number of Bitcoins processed by Client.
      case noofbitcoins:Int=>  
      clientBitcoins+=noofbitcoins
      println("\n\n No of Bitcoins mined by clients is :"+noofbitcoins)      
    }

  }

}
