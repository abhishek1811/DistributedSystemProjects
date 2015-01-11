import akka.actor._

import scala.util.Random
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import scala.collection.mutable.HashMap
import scala.concurrent.ExecutionContext.Implicits.global

sealed trait Messages

case class create(maxID:Int) extends Messages
case class Ready(message:String)extends Messages
case class getDuration(timing:Int)extends Messages
case class getFrequency(frequency:Int)extends Messages
case class schedule(absolutetime:Long)extends Messages
case class AssignIDS(tempst:Int,tempend:Int) extends Messages
case class routeme(myaddress:ActorRef,myuserid:Int,message:String) extends  Messages
case class tweeting(myaddress:ActorRef,myuserid:Int,message:String) extends  Messages

object Client {

	var system = ActorSystem("TwitterClient")

	def IPaddressCheck(IP:String)={
		if(IP==" "){
			println("IP Address is not valid,Sorry try Again...")
			System.exit(0)
		}
	}

	def main(args: Array[String]): Unit = {
	    
	    if(args.length!=3){
			println("-------------------Invalid Input-----------------\nThe command line input for this program is as follow:\n scala Client Ipaddress(Server) ScalingFactor Option(Event/Regular)")
		 	System.exit(0) 
		}

		var sf=if(args.length < 2) 0  else args(1).toInt
		var clients=(1000000*sf)/100
		Global.option=args(2)
		var IPaddress=if((args.length >1)&&(args(0).containsSlice("."))) args(0)  else " "
		IPaddressCheck(IPaddress)
		println(" IPaddress of Server: "+IPaddress+"\n Client Users: "+clients)
	    System.setProperty("java.net.preferIPv4Stack", "true")
	    var clientmaster=system.actorOf(Props(new ClientMaster(clients,IPaddress)),name="clientmaster")
	    clientmaster ! "requestnow"
	
	}

	class ClientMaster(clients:Int,IPaddress:String) extends Actor{
	  	Global.IP=IPaddress
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
	  			var absolutetime 	= System.currentTimeMillis+3000
	  			for(i<-0 to clients-1){
			    	myclients(i)=system.actorOf(Props(new ClientUser(tempst+i,servermaster)),name="clientuser"+(tempst+i)) 	
			    	myclients(i)! schedule(absolutetime)
		    	}

	  		}
	  		case getDuration(timing) => {
	  			
	  			Global.Runningtime=timing
	  			println("Runningtime: "+Global.Runningtime)
	  			var avgtweetspersec=5787
	  			val Avgtweetsperuserforduration=((5787*Global.Runningtime)/clients).toInt
	  			Global.frequency=(Global.Runningtime/Avgtweetsperuserforduration).toInt
	  			if(Global.option=="Regular")
	  			sender ! getFrequency(Global.frequency)
	  			else
	  			sender ! getFrequency(1)
	  		}
	  		case "terminate"=>{
	  			context.system.shutdown
	  		}
	  	
	  	}
	}
	class ClientUser(myid:Int,servermaster:ActorRef) extends Actor{
	  	
	  	var userid:Int=myid
	  	var tweetbank1=new Array[String](100)
	  	val serverworker=context.actorFor("akka://Twitter@"+Global.IP+":8787/user/servermaster/ServerWorker")	
	  	
	  	def receive={
	  		
	  		case Ready(option)=>{
	  			
	  			if(option=="Event"){
	  				context.system.scheduler.schedule(Duration.create(600, TimeUnit.MILLISECONDS),
	         		 Duration.create(1, TimeUnit.SECONDS)) {
			  			var r=Random
			  			var indexofmessage=r.nextInt(100)
			  			serverworker ! tweeting(self,userid,userid+". "+Global.tweetbank(indexofmessage))
		  			}
	  			}
	  			else if(option=="Regular"){
	  					context.system.scheduler.schedule(Duration.create(600, TimeUnit.MILLISECONDS),
	         		 Duration.create(Global.frequency, TimeUnit.SECONDS)) {
			  			var r=Random
			  			var indexofmessage=r.nextInt(100)
			  			serverworker ! tweeting(self,userid,userid+". "+Global.tweetbank(indexofmessage))
		  			}

	  			}
	  		}
	  		case schedule(absolutetime)=>{

	  			var time = absolutetime - System.currentTimeMillis()
	  			context.system.scheduler.scheduleOnce(Duration.create(time, TimeUnit.MILLISECONDS),self,Ready(Global.option))
	  		}
	  	}
	}
	
	object Global{
		
		
		var i=0
		var IP=""
		var option=""
		var frequency=0
		var Runningtime=0
		var tweetbank=new Array[String](100)
 		val filename = "/Users/Abhishek/Desktop/Twitter_1/myfile.txt"
 		for (line <- scala.io.Source.fromFile(filename).getLines()) {
  			tweetbank(i) = line
  			i+=1
	  	}

	}

}
