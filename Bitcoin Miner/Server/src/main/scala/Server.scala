package server
import akka.actor._
import akka.routing.RoundRobinRouter
import java.security.MessageDigest
import akka.event.Logging;
import akka.event.LoggingAdapter;
import common._


case object Start
case object Send
case class msgint(msg:Int)
case class exchange(msg: Int)
case class Message(msg: String)


object Server extends App {
  
  var k=0
  var coincounter=0
  val system = ActorSystem("ServerSystem")
  val terminators = system.actorOf(Props[Terminator], name = "terminators")
  val argument = if(args.length < 1) " "  else args(0)

  sealed trait ProjectOneMessage
  case class collect(x:String)
  case class Over (terminate : Int)
	case object Start extends ProjectOneMessage
  case object FindBitcoin extends ProjectOneMessage 
	case class Done (shaDone : Int) extends ProjectOneMessage
  case class Work (from : Int , end : Int , leadingZeroes : Int) extends ProjectOneMessage
	
	  
	def assign(argument:String)={
		
    if(argument==" ")
		{
      println("*********Invalid Argument**********" +"\n Input should be of type : \n scala Project1 leadingZeroes \n scala Project1 IPAddress" +"\n ***********************************")
      System.exit(1)
		}
		else if(argument.containsSlice(".")){
		println(argument)
		k=0
	  }
		else {
			k=argument.toInt
		}
        
	}

	assign(argument)
	findBitcoin( 8 , 10000000 , k)// making master active

	class Worker extends Actor {

  	def sha_hash(from: Int, end: Int, x: Int) =
    {  
      
      var pattern: String = ""
      var hashstrings: String = ""
      // instantiates the object of sha-256 to sha
      val sha = MessageDigest.getInstance("sha-256")
      var max=1
      // generates pattern as required
      def createPattern(max:Int)={	
        for (j <- 1 to max) {
        pattern = pattern + "0"
        }
      }
      def hasher()={
         	 // generate all hash strings b/w a parametrized set.
        for (i <- from to end - 1) {
  	      // generates all the hex string, foldleft is curried function used on sha list
  	      var hexstring = sha.digest(("n.sadhwani" + i.toString()).getBytes).foldLeft("")((s: String, b: Byte) => s +
  	      Character.forDigit((b & 0xf0) >> 4, 16) + Character.forDigit(b & 0x0f, 16))
  	      // select all the hash strings which starts with required pattern
  	      if (hexstring.startsWith(pattern)) {
  	       hashstrings += "n.sadhwani" + i + " " + hexstring + "\n"
  	       coincounter +=1
  	       println(hashstrings)
  	      }
        }

      }
         
      if(x==0){
      createPattern(max)
      hasher()
      }
      else if(x>0){
      createPattern(x)
      hasher()
      }
      Done(coincounter)

    }

    def receive = {
    case Work(from, end , leadingZeroes) ⇒
    sender ! sha_hash(from, end , leadingZeroes)
    }

  }
  
	class Master(noOfWorkers : Int , TotalElements : Int , leadingZeroes : Int, terminator: ActorRef) extends Actor 
	{
    var clientbitcoins=0 
 	  var max_i = (TotalElements/10000)
	  var end=11000000
    var from=10000000
    var buffer=1000000
    val clientworker=2
    val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(noOfWorkers)), name = "workerRouter")
 	  var counter = 1
    var clientcount=1
    def receive = {
    	
      case "Ready" => 	
       println ("I am sending")
       sender !  from
       sender !  end
       sender !  leadingZeroes
       sender !  clientworker 
       from=from+buffer
       end=end+buffer
      
      case FindBitcoin =>
    	 for (i <-1 until max_i)
    	 {
    		 workerRouter ! Work((i-1) * 10000, i * 10000, k)	
    	 }
	
     	case Done(shaDone) => 
  		 counter += 1;
  		 if(counter == max_i){
  			println("Total Bit Coins Mined :" + (shaDone+clientbitcoins) + "\n Terminating..........")
  			terminators ! Over(1)
  		  context.stop(self)		
       }
      	// print the strings accepted from client
      case msg: String => 
       
			 println("*****---------Bitcoins received from client "+clientcount+"-----------------------*********")
			 clientcount=clientcount+1
       println(msg.toString) 	
			 println("*********------------------------------------------------------------------*****************")

      case noofbitcoins:Int=>  
       clientbitcoins+=noofbitcoins
       println("\n\n No of Bitcoins mined by client 1 is :"+noofbitcoins)         
    }
  
  }
 
  
	class Terminator extends Actor{
    var temp=0
    def receive = {
    case Over(terminate) ⇒
     println("-------------------")
     context.system.shutdown()
    }                          
  }
	

	def findBitcoin(noOfWorkers : Int , TotalElements : Int , leadingZeroes : Int){
   val master = system.actorOf(Props(new Master(noOfWorkers, TotalElements, leadingZeroes , terminators)),name = "master") 
    println(master.path)
		master ! FindBitcoin
	}
	
}
