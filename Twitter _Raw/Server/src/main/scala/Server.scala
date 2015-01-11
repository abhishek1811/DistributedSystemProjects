import akka.actor._

import scala.util.Random
import scala.concurrent.duration._
import java.util.concurrent.TimeUnit
import akka.routing.RoundRobinRouter
import scala.concurrent.ExecutionContext.Implicits.global
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.HashMap

//import Common._  &&// change arguments s_Actors no use
sealed trait Messages
case class create(maxID:Int) extends Messages
case class AssignIDS(tempst:Int,tempend:Int) extends Messages
case class routeme(myaddress:ActorRef,myuserid:Int,message:String) extends  Messages
case class tweeting(myaddress:ActorRef,myuserid:Int,message:String) extends  Messages

object Server {

  def main(args: Array[String]): Unit = {
    if(args.length!=3){
  		println("-------------------Invalid Input-----------------\nThe command line input for this program is as follow:\n scala Project4  scalingfactor #ofserveractor ClientMachine");
  	 	System.exit(0) 
	  }
  	val sf=args(0).toInt
  	val s_actors=((Runtime.getRuntime().availableProcessors())*1.5).toInt
  	val maxclients=args(2).toInt// equal to watingcounter
  	println(" Scaling Factor: "+sf+"\n Max Server Actors: "+s_actors+"\n Max Client Machine: "+maxclients)
  	val config = ConfigFactory.parseString("akka.remote.netty.tcp.port=8787").withFallback(ConfigFactory.load()).resolve
  	var system=ActorSystem("Twitter",config)
  	System.setProperty("java.net.preferIPv4Stack", "true")
  	var servermaster=system.actorOf(Props(new ServerMaster(maxclients,s_actors)),name="servermaster")
  	println(servermaster.path)
  	servermaster ! "Initiate"
	
  }
  class ServerMaster(maxclients:Int,s_actors:Int) extends Actor{
  	var waiting=0 
  	var startid=0
  	var endid=0
  	var tempst=0
  	var tempend=0
  	var clientref:Array[ActorRef]=_
    val serverworker = context.actorOf(Props[ServerWorker].withRouter(RoundRobinRouter(s_actors)), name = "ServerWorker")
  	var totalusers=0

  	def printme(a:Array[ActorRef])={
  	 	for(i<-0 to a.length-1){
  			println(a(i))
  		}
  		println()
  	}

  	def assignStats():Array[Double]={
	
  		var percent=Array(10,20,30,40,50,60,70,80,90,95,96,97,98,99,99.9)
  		var key:Array[Double]=new Array[Double](16)
  		for(i<-0 to 14){
  			key(i)=(percent(i)*totalusers*.01)	
  		}
  		return key
  	}

  	def relations()={
  		var nooffollowers=Array(3,9,19,36,61,98,154,246,458,819,978,1211,1675,2991,24964)
      //var nooffollowers=Array()
  		var stats=assignStats()
  		var count=0
  		var from =0
  		var till=nooffollowers(count)
  		var r=Random
  		var randomfound=0
  		var t=0
  		println("Total Users: "+totalusers )
  		for (i<-0 to Global.user.size-1){
  			t=t+1
  			//var percent:Float=(i*100)/totalusers
  			println(t+". Creating :"+i)
  			if(i>stats(count)){
  				if(count>=14)
  				  till=50000
  				else{
  					from=till
  					count=count+1
  					till=nooffollowers(count)
  				}	
  			}
  			
  			if(i<=stats(count)||(till==50000)){// boundary case && zero followers
  				val range=from to till
  				var rand=range(r.nextInt(range.length))// apni id bhi tog utha sakta hai and fix if rand in already in set then what.
  				for(j<-0 to rand-1){//zero followers
  					randomfound=r.nextInt(totalusers)
  					Global.user(i).followers+=randomfound 
  					Global.user(randomfound).following+=Global.user(i).userid
  				}
  			
  			}
  		}
  			
  	}
  	def receive={
  		case "Initiate"=>{
  			println("ServerMaster(172.16.102.138) waiting for Clients....")
  			clientref=new Array[ActorRef](maxclients)

  		}
      case routeme(myaddress,myuserid,message)=>{
        serverworker ! tweeting(myaddress,myuserid,message)

      }
  		case create(maxID) =>{// what about when maxid is not same for all machine
  			totalusers=maxclients*maxID
  			if(waiting <=maxclients){
  				endid+=maxID
	  			waiting+=1
	  			println("Creating Userid's for Client: "+waiting )
	  			clientref(waiting-1)=sender
	  			while(startid!=endid){
	  				Global.user+=(startid->new Users(startid))
	  				//println(user(startid).userid)
	  				startid+=1
	  			}
  			
  			}
  			if(waiting==maxclients){
  				println("Server Master is now creating the relations (Followers/Following)")
  				printme(clientref)
  				relations()
  				for(i<-0 to clientref.length-1){
  					println("Server Master is now sending the ids")
  					tempst=tempend
  					tempend+=maxID
  					clientref(i) ! AssignIDS(tempst,tempend)
  				}	
  				for(i<-0 to Global.user.size-1){
           println("----------------------------------------------------------------------------------------------------")
    				println ("ID:"+i+"\nFollowers: "+ Global.user(i).followers.size+"\nFollowing: "+Global.user(i).following.size) 
            
            println("----------------------------------------------------------------------------------------------------")
          }
          context.system.scheduler.scheduleOnce(60.seconds){
            /*for(i<- 0 to Global.user.size-1){
                println("userid " +i+"\n---------------------------")

                for(j<-0 to Global.user(i).tweets.size-1){
                  println(Global.user(i).tweets(j))
                }
                println("\n---------------------------")
            }*/
            println("Message Processed in 60 sec: "+Global.messageprocessed/60)
            context.system.shutdown
          }

  			}

  			
  		}


  	}
  }
  class ServerWorker extends Actor{// you can take these parameter as a function also.
  	def receive={
  		case tweeting(myaddress,myuserid,message)=>{
        Global.user(myuserid).storemymessage(message)
       // println(myuserid+" "+message)
        for(x<-Global.user(myuserid).following)
        Global.user(x).storemymessage(message)
        Global.messageprocessed=Global.messageprocessed+1

      }

  	}
  }
  class Users(id:Int){
  	var userid:Int=id
  	var followers:Set[Int]=Set()
  	var following:Set[Int]=Set()
  	var tweets:Array[String]=new Array[String](100)
    var messagecounter = -1
    def storemymessage(message:String)={
      if(messagecounter==99)
         messagecounter= -1
      messagecounter=messagecounter+1
      tweets(messagecounter)=message
      
    }
  }
  object Global{
    var user=HashMap.empty[Int,Users]
    var messageprocessed=0

  }

}
