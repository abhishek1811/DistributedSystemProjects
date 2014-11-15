import akka.actor._
import scala.util.Random
import util.control.Breaks._
sealed trait Messages
case class message(noofnodes:Int,topology:String,algorithm:String) extends Messages
case class construct(topology:String)extends Messages
case class Gossip(startnode:Int,neighbors:Array[Array[Int]],structure:Array[ActorRef]) extends Messages
case class dones(i:Int) extends Messages
case class Pushsum(s:Double,w:Double,neighbors:Array[Array[Int]],structure:Array[ActorRef],startnode:Int)extends Messages
case class over(i:Double) extends Messages
case class Assign(i:Int,structure:Array[ActorRef])extends Messages

object Project2 extends App{
		
	    if (args.length!=3){
	    	println("**************Invalid Input*********************\nThe command line input for this program is as follow:\nNoofnodes Topology Algorithm");
	        System.exit(0)
	    }
		var noofnodes=args(0).toInt
	    var topology=args(1)
	    var algorithm=args(2)
	    var system=ActorSystem("Gossip-Pushsum")
	    var master:ActorRef=system.actorOf(Props[Master],name="master")
	    master ! message(noofnodes,topology,algorithm)

	class Master extends Actor{
		
		var noofnodes=0;
		var structure:Array[ActorRef]= _
		var start:Long=0
		var done=0
		var counter:Array[Int]= _
		var average=0.0
		def receive={
			case message(nodes,topology,algorithm) =>{// making structure of network
				println("Input Received:\n"+nodes+" "+topology+" "+algorithm)
				noofnodes=nodes
				structure=new Array[ActorRef](nodes)
				for(i<- 0 until structure.length){
	                structure(i) = system.actorOf(Props(new Nodes()), name = "node" + i)
	                structure(i) ! Assign(i,structure)
				}
				println("Creating Topology.......")
				counter=new Array[Int](noofnodes)
				val neighbors:Array[Array[Int]]=construct(topology)
	            println("Size of neighbor"+neighbors(0).length)
				if(algorithm=="gossip"){
					start= System.currentTimeMillis
					var startnode=Random.nextInt(noofnodes)
					println("In gossip---> startnode is: "+startnode)
		            structure(startnode)! Gossip(startnode,neighbors,structure)// i am sending startnode in argument to extract neighbor of sending node .
		    		var stop:Long = System.currentTimeMillis  
		    		println("Time is"+(stop-start))
				}
				else if(algorithm=="pushsum"){
                    start= System.currentTimeMillis
                    var startnode=Random.nextInt(noofnodes)
                    structure(startnode) ! Pushsum(0,1,neighbors,structure,startnode)
				}
			}
			case dones(i)=>{
				done=done+1
				println("************* i " + i)
				counter(i)=1
				for(i<-0 to noofnodes-1){
                    print(counter(i))
				}
				println("\n")
				if(done==noofnodes)
				{	var stop:Long = System.currentTimeMillis  
					println("*********Convergence********************")
					println("Runtime : " + (stop - start))
					context.system.shutdown
				}
			}
			case over(i)=>{
              done=done+1
              average+=i
              //println("************* i " + i)
				//counter(i)=1
				//for(i<-0 to noofnodes-1){
                    //print(counter(i))
				//}
				//println("\n")
				if(done==noofnodes)
				{	var stop:Long = System.currentTimeMillis  
					println("*********Convergence********************")
					println("Average of Network is :"+average/noofnodes)
					println("Runtime : " + (stop - start))
					context.system.shutdown
				}

			}

		}

		def construct(topology:String):Array[Array[Int]]={
	        var neighbors=Array.ofDim[Int](noofnodes,noofnodes)
			if((topology=="full")||(topology=="Full")){

				println("Full Topology.......")
				neighbors=Array.ofDim[Int](noofnodes,noofnodes)
				for(i<-0 until noofnodes){
					for(j<-0 until noofnodes ){
	                    if(i==j){

	                    }
	                    else{
	                    	neighbors(i)(j)=j
						    println(neighbors(i)(j))
	                    }
					}
					println("\n")	
				}
			}
			else if((topology=="line")||(topology=="Line")){
			    println("Line Topology....")
			    neighbors=Array.ofDim[Int](noofnodes,2)
		        for(i<-0 until noofnodes){
		        	println("Neigbours of"+i+"are ")
		        	for(j<-0 to 1 ){
		        		if(i==0){
		        			neighbors(i)(j)=i+1
		        			 print(neighbors(i)(j))
		        		}
		        		  
		        		if(i==noofnodes-1){
		        			neighbors(i)(j)=i-1 
		        			print(neighbors(i)(j))
		        		}
		        		 
		        		 
		        		if((j==0)&&(i>0)&&(i<(noofnodes-1))){
		        			neighbors(i)(j)=i-1
		                    print(neighbors(i)(j))
		        		}
		                 
		                else if((j>0)&&(i<(noofnodes-1))&&(i>0)){
		                	neighbors(i)(j)=i+1
		                    print(neighbors(i)(j))
		                }
		                 
		        	}
		        	println("\n")
		        }
			}
			else if((topology=="2D")||(topology=="2D")){
				println("2D Topology....")
				neighbors=Array.ofDim[Int](noofnodes,4)


			}
			else if ((topology=="imp2D")||(topology=="Imp2D")){
				println("Imp2D Topology....")
				neighbors=Array.ofDim[Int](noofnodes,5)

			}
			return neighbors
		}
	}

	class Nodes extends Actor{
		
		var neighbors:Array[Array[Int]]= _ // Error here what about when neighbors matrix size vary in case of other topology
		var startnode=0
		var noofnodes=0
		//var structure:Array[ActorRef]= _
		var counter=0
		var done=0
		var status=0
		var gossipcounter=0
		var mys=0.0
		var myw=1.0
		var prevs=0.0
		var prevw=1.0
		var random=0
		var loop=0
		var count=0

	  	def find(actor:ActorRef,structure:Array[ActorRef]):Int={
	    	var i=0
		        for(i<-0 to structure.length){
		            if(structure(i)==self)
		            return i
		        } 
		    return i
		}   
		def receive={

			case Gossip(startnode,neighbors,structure)=>{

				val noofnodes=neighbors(startnode).size
				if(status==0){
					master! dones(startnode)
					status=1
				}
		 		val random = Random.nextInt(neighbors(startnode).size)
		 		var i=neighbors(startnode)(random)		 		
			 	if(sender!=self){
			 		gossipcounter=gossipcounter+1
			 	}
   			    structure(i)! Gossip(i,neighbors,structure)
			    if(gossipcounter<=10) {
	               var j=find(self,structure)
			       self! Gossip(j,neighbors,structure)
			    }

			}
			case Pushsum(s,w,neighbors,structure,startnode)=>{
	            
	            mys=(mys+s)/2
	            myw=(myw+w)/2
	           
	            if(Math.abs((mys/myw)-(prevs/prevw))<Math.pow(10,-10)){
                   count=count+1
                   if(count==3)
                   master ! over(mys/myw)
	            }
	            else count=0
	            
	            random = Random.nextInt(neighbors(startnode).size)
		 		var i=neighbors(startnode)(random)
	            structure(i) ! Pushsum(mys,myw,neighbors,structure,i)
                prevs=mys
	            prevw=myw
                
			}
			case Assign(i)=>{
				mys=i
			}
			
		}

		
	}

}

