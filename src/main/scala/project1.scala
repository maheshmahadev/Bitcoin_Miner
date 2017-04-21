import java.security.MessageDigest
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.routing.RoundRobinPool


/*on the basis of user requirement of leading zeros as argument, process will start*/
case class beginBitcoinMining(no_of_zeros: Integer)

/*After each worker unit(100000) is processed, bitcoins received from the workers are stored in a buffer*/
case class minedBitcoins(mined_coins:ArrayBuffer[String])

/*to display summary of mining session and bitcoins found if any*/
case class doneMining(inputsprocessed:Integer)

/*master will invoke 14 workers to distribute the work*/
case class assignWork(no_of_zeros:Integer)


/*
1. User can give required number of leading zero if user want to mine
bitcoins
2. User can give ip address of remote system as cmdline argument,
It will connect to remote system on port specified*/
object project1 extends App {

  var command_line_args:String=args(0)
  if(command_line_args.contains('.'))
  {
    val worker =ActorSystem("workersystem").actorOf(RoundRobinPool(8).props(Props[Worker]))
    for (n <- 1 to 8)
      worker ! command_line_args
  }
  else
  {
    var no_of_zeros=args(0).toInt
    val system = ActorSystem("master")
    val master = system.actorOf(Props[Master],name="Master")
    master ! assignWork(no_of_zeros)
    println("invoke the master")
  }
}

class Worker extends Actor {

  def receive = {
    /*Master will generate random seeds and concatenate it input string
    prefixed by the gatorlink ID of one of the team member.
    Genereated strings are given as input to get hashcode using SHA-256.
    used java.security.MessageDigest library.
    The mined bitcoins are passed to the master after a work unit of 100000
     is processed
    and bitcoins are stored in an array buffer.
    Process will come to halt after 5 min*/


    case beginBitcoinMining(no_of_zeros:Integer) => {
      var mined_coins:ArrayBuffer[String]=  ArrayBuffer[String]()
      var inputsprocessed:Integer=0
      val beginTime=System.currentTimeMillis()
      var seed1:String=Random.alphanumeric.take(6).mkString
      var seed2:String=Random.alphanumeric.take(6).mkString
      while(System.currentTimeMillis()-beginTime<300000){
        var s:String = "shantanu.kande"+seed1+seed2+inputsprocessed
        val sha = MessageDigest.getInstance("SHA-256")
        var bitcoin:String=sha.digest(s.getBytes).foldLeft("")((s:String, b: Byte) => s + Character.forDigit((b & 0xf0) >> 4, 16) +Character.forDigit(b & 0x0f, 16))
        var extracted_val:String=bitcoin.substring(0,no_of_zeros)
        var comparison_val="0"*no_of_zeros
        if(extracted_val.equals(comparison_val)){
          mined_coins+="shantanu.kande"+seed1+seed2+inputsprocessed+" "+bitcoin
        }
        inputsprocessed+=1
        if(inputsprocessed%100000==0)

          sender ! minedBitcoins(mined_coins)
      }
      sender ! doneMining(inputsprocessed)
    }
    /* if cmdline argument is ip address, it will connect to that remote system */
    case ipaddress:String => {
      println("inside remote worker")
      val master=context.actorSelection("akka.tcp://master@"+ipaddress+":5052/user/Master")
      master! "remote"
    }

  }
}



class Master extends Actor {

  private var num_of_zeros:Integer=0
  //private var noofbitcoins:Integer = 0
  private var worknum:Integer=0
  private var inputs_processed:Integer=0
  private var num_of_workers:Integer=0
  private var total_mined_bitcoins:ArrayBuffer[String]=  ArrayBuffer[String]()
  def receive = {
    /*master will invoke 14 workers*/
    case assignWork(no_of_zeros:Integer) => {
      num_of_zeros=no_of_zeros
      num_of_workers+=14
      println("invoke the worker")
      val worker =context.actorOf(RoundRobinPool(14).props(Props[Worker]))
      for (n <- 1 to 14)
        worker ! beginBitcoinMining(num_of_zeros)
    }
    /* after each worker unit(100000) is processed,
    available bitcoins get stored in buffer*/
    case minedBitcoins(mined_coins:ArrayBuffer[String]) => {
      total_mined_bitcoins++=mined_coins
      println("Worker reported progress about mining")

    }
    /* Master will invoke 8 worker in remote system*/
    case "remote" => {
      println("remote worker active")
      num_of_workers+=1
      sender ! beginBitcoinMining(num_of_zeros)
    }

    /*System is shutdown after all worker finished processing
    or if time limit(5 mins) is met.
    It will print all mined bitcoins*/

    case doneMining(inputsprocessed:Integer) =>
    {
      worknum+=1
      inputs_processed+=inputsprocessed
      if(worknum == num_of_workers)
      {
        println("Number of workers : "+num_of_workers)
        println("Number of inputs processed : "+inputs_processed)
        total_mined_bitcoins=total_mined_bitcoins.distinct
        for(i<- 0 until total_mined_bitcoins.length )
          println((i+1)+" " + total_mined_bitcoins(i))
        println("Number of bitcoins found : "+total_mined_bitcoins.length )
        context.system.shutdown()
      }
    }
  }
}