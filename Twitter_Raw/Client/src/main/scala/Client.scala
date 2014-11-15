import akka.actor._
import scala.util.Random
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global

//import Common._
sealed trait Messages
case class create(maxID:Int) extends Messages
case class AssignIDS(tempst:Int,tempend:Int) extends Messages
case class routeme(myaddress:ActorRef,myuserid:Int,message:String) extends  Messages


object Client {
	def IPaddressCheck(IP:String)={
		if(IP==" "){
			println("IP Address is not valid,Sorry try Again...")
			System.exit(0)
		}
	}
	var system = ActorSystem("TwitterClient")
	def main(args: Array[String]): Unit = {
	    
	    if(args.length!=2){
			println("-------------------Invalid Input-----------------\nThe command line input for this program is as follow:\n scala Client Ipaddress(Server) ScalingFactor")
		 	System.exit(0) 
		}
		var sf=if(args.length < 2) 0  else args(1).toInt
		var clients=(1000000*sf)/100

		var IPaddress=if((args.length >1)&&(args(0).containsSlice("."))) args(0)  else " "
		IPaddressCheck(IPaddress)
		println(" IPaddress of Server: "+IPaddress+"\n Client Users: "+clients)
		
		/*
			*Actor system and Akka actor building start
		 */
		
	    System.setProperty("java.net.preferIPv4Stack", "true")
	    var clientmaster=system.actorOf(Props(new ClientMaster(clients,IPaddress)),name="clientmaster")
	    clientmaster ! "requestnow"
	    
		//System.exit(0)	
	}
	class ClientMaster(clients:Int,IPaddress:String) extends Actor{
	  	val servermaster = context.actorFor("akka://Twitter@"+IPaddress+":8787/user/servermaster")
	  	println("Master reference defined "  + servermaster)
	  	var myclients:Array[ActorRef]=new Array[ActorRef](clients)
	  	var start:Long=0
	  	var end:Long=0
	  	def receive={
	  		case "requestnow" =>{
	  			println(self.path+" In requestnow")
	  			servermaster ! create(clients)
	  		}
	  		case AssignIDS(tempst,tempend) =>{
	  			start = System.nanoTime();
	  			for(i<-0 to clients-1){
			    	myclients(i)=system.actorOf(Props(new ClientUser(tempst+i,servermaster)),name="clientuser"+(tempst+i)) 	
			    	myclients(i)! "Ready"
		    	}
		    	//self ! "terminate"
	  		}
	  		case "terminate"=>{
	  			end = System.nanoTime();
	  			System.out.println("Took: " + ((end - start) / 1000000) + "ms");
	  			context.system.shutdown
	  		}
	  		case "Tweet"=>{ 

	  		}
	  	}
	}
	class ClientUser(myid:Int,servermaster:ActorRef) extends Actor{
	  	var userid:Int=myid
	  	var tweetbank1=new Array[String](100)
	  	//val master = context.actorFor("akka://ServerSystem@"+args(0)+":6565/user/master")
	  	def readmyfile()={
	  		//var messages = new Array[String](100)
 			var i=0
 			val filename = "/Users/Abhishek/Desktop/myfile.txt"
 			for (line <- scala.io.Source.fromFile(filename).getLines()) {
  				tweetbank1(i) = line
  				i+=1
	  		}
	  		//return messages
	  	}	
	  	def receive={
	  		case "Ready"=>{
	  			//println(self.path+" ID :"+userid)
	  			context.system.scheduler.schedule(Duration.create(600, TimeUnit.MILLISECONDS),
         		 Duration.create(1, TimeUnit.SECONDS)) {
	  			var r=Random
	  			var indexofmessage=r.nextInt(100)
	  			servermaster ! routeme(self,userid,Global.tweetbank(indexofmessage))
	  			}
	  		}

	  	}
	}
	object Global{
		var tweetbank=new Array[String](100)
		var i=0
 		val filename = "/Users/Abhishek/Desktop/Twitter/myfile.txt"
 		for (line <- scala.io.Source.fromFile(filename).getLines()) {
  			tweetbank(i) = line
  			i+=1
	  	}

	}
	

}



