package client
import akka.actor._
import common._
import akka.event.Logging
import akka.event.LoggingAdapter
import java.security.MessageDigest
import akka.routing.RoundRobinRouter

case object Start
case object Send
case class msgint(msg:Int)
case class exchange(msg: Int)
case class Message(msg: String)

object Client extends App {

  var k=0
  var coincounter=0
  val system = ActorSystem("ClientSystem")
  System.setProperty("java.net.preferIPv4Stack", "true")
  val listener =system.actorOf(Props[Listener], name = "Listener")
  val terminator = system.actorOf(Props[Terminator], name = "terminator")
  case object Ready   
	sealed trait ProjectOneMessage
	case object FindBitcoin extends ProjectOneMessage
  case class Over (terminate : Int)
  case class Done (shaDone : Int, hashstrings :String) extends ProjectOneMessage
  case class Work (from : Int , end : Int , leadingZeroes : Int) extends ProjectOneMessage

	var from=0
  var end=0
  var zeroes=0
  var noOfWorkers=0
  val argument = if(args.length < 1) " "  else args(0)

  def assign(argument:String)={
    if(argument.containsSlice(".")){
      println("IP ADDRESS OF SERVER : "+argument)
      k=0
    }
    else 
     {
      println("*********Invalid Argument**********" + "\n Input should be of type : \nscala Project1 IPAddress" +"\n ***********************************")
      System.exit(1)
    }

  }
  assign(argument)
  listener! "i am alive" 

	class Worker extends Actor {
    var pattern: String = ""
    var max=1
    def receive = {
      case Work(from, end , leadingZeroes) ⇒
        println("Worker geting work..............\n")
        sender ! sha_hash(from, end , leadingZeroes)
    }
    
    def sha_hash(from: Int, end: Int, x: Int) =
	  {
      var totalString:String=""
      val sha = MessageDigest.getInstance("sha-256")  // instantiates the object of sha-256 to sha  
      var hashstrings: String = ""
      pattern=""
      for (j <- 1 to x) {
          pattern = pattern + "0"
      }

      // generate all hash strings b/w a parametrized set.
      for (i <- from to end - 1) {
        // generates all the hex string, foldleft is curried function used on sha list
        var hexstring = sha.digest(("n.sadhwani" + i.toString()).getBytes).foldLeft("")((s: String, b: Byte) => s +
          Character.forDigit((b & 0xf0) >> 4, 16) + Character.forDigit(b & 0x0f, 16))
        // select all the hash strings which starts with required pattern
        if (hexstring.startsWith(pattern)) {
          hashstrings = "n.sadhwani" + i + " " + hexstring + "\n"
          coincounter +=1
          totalString=totalString+hashstrings
        }
      }
      println(totalString+"\n---------------------------------------------------------------\n")   
	    totalString
	  }

  }
	class Master(noOfWorkers : Int , from : Int , end : Int ,  leadingZeroes : Int, terminator: ActorRef)extends Actor {
    
    var netstring:String=""
 	  var max_i = 10
    var set=(end-from)/max_i
    var counter = 1
    var buffer = from
    var noofbitcoins=0
    val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(noOfWorkers)), name = "workerRouter")

    def receive = {
    	//Server will send message to client to start       
      case FindBitcoin ⇒ 
        println("Within FindBitcoins")
      	for (i ← 1 until max_i){
      		workerRouter ! Work(buffer, buffer + set, leadingZeroes)
      		buffer = buffer + set
      	}

			case hashstrings: String  =>
        counter+=1
        if(hashstrings!=""){

          noofbitcoins+=1
          netstring=netstring+hashstrings
				}
        if(counter == max_i){
          println("Bitcoins ="+coincounter)
          def fin:Int=coincounter.toInt
          listener ! msgint(fin) 
					listener ! netstring
        } 		
    }
  }
 
  
	class Terminator extends Actor {
    def receive = {
      case Over(terminate) ⇒
      	println("-------------------")
        context.system.shutdown()
    }
  } 
   // Listener listens to  server master and it get activated when we execute client
	class Listener extends Actor{

    val master = context.actorFor("akka://ServerSystem@"+args(0)+":6565/user/master")
    println("Master reference defined "  + master)
    var counter=0

    def receive= {

      case msg: Int=> 
        println("Receiving Input From Master")
        if(counter==0){
          from=msg
        	println("\nFrom :"+from)
        	counter +=1
        }
        else if(counter==1){
        	end=msg
        	println("\nEnd:"+end)
        	counter+=1
        }
        else if (counter==2){
        	zeroes=msg
        	println("\nZeroes :"+zeroes)
        	counter+=1
        }
        else if (counter==3)
        {
         noOfWorkers=msg
         println("\nnoOfWorkers:"+noOfWorkers)
         findBitcoin( noOfWorkers , from , end , zeroes)//activates  find bit coin 
         println("\n Bitcoin Searching......................................................")
        }
      // first message to server master       
      case "i am alive"  =>   
        println("Server Master address"+ master )
        master ! "Ready"
        println("I am alive ") 
               
      case msg: String => 
        master ! msg
        println("--------------------------------BITCOINS FINED IN CLIENT---------------------------------------", msg)  
        terminator ! Over(1)
        context.stop(self)   

      case msgint(noofbitcoins) =>  
        println("NO OF BITCOINS MINED BY CLIENT IS :"+ noofbitcoins)
        master ! noofbitcoins
  	}
	}

	def findBitcoin(noOfWorkers : Int , fromm : Int, end : Int , leadingZeroes : Int){

		val remotemaster = system.actorOf(Props(new Master(noOfWorkers, fromm , end, leadingZeroes , terminator)),name = "remotemaster")
		remotemaster ! FindBitcoin
	}
	
}
