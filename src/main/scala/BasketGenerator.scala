/**
 * Created under license Apache 2.0
 * User: nthakur
 * Date: 02/05/12
 * Time: 10:26
 * 
 */

import akka.actor._
import akka.routing.RoundRobinRouter
import com.typesafe.config.ConfigFactory

object BasketGenerator extends App{
  generate()

  // Message classes
  // sealed stops this trait from being used outside this module.
  // trait allows a mix of abstract and concrete methods - a mixin which allows implementation reuse.
  sealed trait SimulatorMessage
  //case classes/objects can be used for pattern matching, have compiler generated toString, equality, constructor methods.
  case object SimulationComplete extends SimulatorMessage
  case object SimulationStart extends SimulatorMessage
  case object GoShop extends SimulatorMessage
  case object ShopperDied extends SimulatorMessage

  class Shopper extends Actor with ActorLogging{
    val myId: String = nextASCIIString(10)

    def receive = {
      case GoShop =>
        log.info("I'm Shopping ({})!", myId)
        // todo: generate shopper id from database
        // todo: generate basket from database
        // terminate this shopper's stint
        sender ! ShopperDied
    }

    def nextASCIIString(length: Int) = {
      val (min, max) = (33, 126)
      def nextDigit = util.Random.nextInt(max - min) + min

      new String(Array.fill(length)(nextDigit.toByte), "ASCII")
    }
  }

  class Shop(noOfShoppers: Int, controller: ActorRef) extends Actor with ActorLogging {
    // We round-robin start requests to each Shopper
    val shopperRouter = context.actorOf(Props[Shopper].withRouter(RoundRobinRouter(noOfShoppers)), name = "shopperRouter")
    // Count number of shopper's leaving the store.
    var noOfShoppersDied: Int = _

    def receive = {
      case SimulationStart =>
        log.info("Starting Simulation")
        for (i <- 0 until noOfShoppers) shopperRouter ! GoShop
      case ShopperDied =>
        noOfShoppersDied += 1
        if (noOfShoppersDied == noOfShoppers) {
          controller ! SimulationComplete
          context.stop(self)
        }
    }
  }

  class Controller extends Actor with ActorLogging{
    def receive = {
      case SimulationComplete =>
        log.info("Simulation Finished.")
        // context is the current Actor and system returns the ActorSystem this Actor belongs to.
        context.system.shutdown()
    }
  }
  def generate(){
    val config = ConfigFactory.load()
    val noOfShoppers = config.getInt("basket_generator.noOfShoppers")
    Console.printf("Generator simulating %s shoppers.", noOfShoppers)

    val actorSystem = ActorSystem("BasketGeneratorSystem", config)

    // used to control shutdown of the system when simulation finishes.
    val controller = actorSystem.actorOf(Props[Controller], name = "controller")
    // master controls the slaves/workers
    val shop = actorSystem.actorOf(Props(new Shop(noOfShoppers, controller)), name = "master")

    shop ! SimulationStart
  }
}
