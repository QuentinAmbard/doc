import java.util.UUID
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import java.util.concurrent.{ArrayBlockingQueue, RejectedExecutionHandler, ThreadPoolExecutor, TimeUnit}

import AsynchRDDHelper.AsynchConfig
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.collection.{Iterator, mutable}
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.reflect.ClassTag



/**
  * Asynch Iterator
  */
class AsynchPartitionIterator[A](iterator: Iterator[A])(implicit ec: ExecutionContextExecutor) extends Iterator[A] {
  def mapAsync[B](f: A => B): Iterator[Future[B]] = new Iterator[Future[B]] {
    def hasNext = iterator.hasNext

    def next() = {
      val next = iterator.next()
      Future {
        f(next)
      }(ec)
    }
  }

  def foreachAsync[U](f: A => U) {
    while (hasNext) {
      val next = iterator.next()
      Future {
        f(next)
      }(ec)
    }
  }

  override def hasNext: Boolean = iterator.hasNext

  override def next(): A = iterator.next()
}


/**
  * Thread pool holder, to share a specific thread pool.
  * Allow to create 1 single thread pool per executor jvm.
  */
object ThreadPoolHolder {
  private val lock = new Object()
  private var pools: mutable.Map[String, ThreadPool] = mutable.Map()

  private case class ThreadPool(exec: ThreadPoolExecutor, context: ExecutionContextExecutor, var count :Int = 1)

  def getInstance(id: String, poolSize: Int) = {
    lock.synchronized {
      if (!pools.contains(id)) {
        val executorWaiting = new ThreadPoolExecutor(poolSize / 2, poolSize + 1, 1, TimeUnit.MINUTES, new ArrayBlockingQueue[Runnable](poolSize * 2), new CallerRunsPolicy())
        pools(id) = ThreadPool(executorWaiting, ExecutionContext.fromExecutor(executorWaiting))
      } else {
        val pool = pools(id)
        pool.count += 1
      }
      pools(id).context
    }
  }

  def closeInstance(id: String) = {
    lock.synchronized {
      val pool = pools(id)
      if(pool.count == 1) {
        pool.exec.shutdown()
        pool.exec.awaitTermination(1, TimeUnit.SECONDS)
        pools.remove(id)
      } else {
        val pool = pools(id)
        pool.count -= 1
      }
    }
  }
}


/**
  * Batch rows and execute them in an asynch fashion.
  */
object AsynchRDDProcessor {

  def processBatch[RESULT, ROW](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit), partition: Iterator[ROW],
                                asynchConfig: AsynchConfig) = {
    val threadPool = ThreadPoolHolder.getInstance(asynchConfig.poolId, asynchConfig.poolSize)
    val asynchIt: AsynchPartitionIterator[ROW] = new AsynchPartitionIterator[ROW](partition)(threadPool)
    val (futureIterator, close) = f(asynchIt)
    val closeAll = () => {
      ThreadPoolHolder.closeInstance(asynchConfig.poolId)
      close()
    }
    val results = slidingPrefetchIterator(futureIterator, asynchConfig.batchSize/2, closeAll)
    results

    //Old solution:
    //    // Perform the sliding technique to queue & process only a fixed amount of records at a time
    //    val slidingIterator = futureIterator.sliding(asynchConfig.batchSize - 1)
    //    val (initIterator, tailIterator) = slidingIterator.span(_ => slidingIterator.hasNext)
    //    initIterator.map(futureBatch => {
    //      Await.result(futureBatch.head, asynchConfig.timeout seconds)
    //    }) ++
    //      tailIterator.flatMap(lastBatch => {
    //        val r = lastBatch.map(future => Await.result(future, asynchConfig.timeout seconds))
    //        close()
    //        r
    //      })

  }

  /**
    *  Prefetches a between batchSize and batchSize*2elements at a time
    *  Example with batchSize = 3
    * [1,2,3,4,5,6,7,8,9]                 //Incoming futures
    * [1,2,3][4,5,6][7,8,9]               // Grouped iterator
    * [[1,2,3][4,5,6]],[[4,5,6][7,8,9]]   //Sliding iterator
    * [1,2,3,4,5,6,7,8,9]                 // Flattened iterator
    */
  protected def slidingPrefetchIterator[T](it: Iterator[Future[T]], batchSize: Int, close: () => Unit): Iterator[T] = {
    val (firstElements, lastElement) =  it
      .grouped(batchSize)
      .sliding(2)
      .span(_ => it.hasNext)
    val firstGroups: Iterator[Seq[Future[T]]] = firstElements.map(_.head)
    val futures: Iterator[Future[T]] = (firstGroups ++ lastElement.flatten).flatten
    futures.map(future => {
      val r = Await.result(future, 10 seconds)
      if(!futures.hasNext){
        close()
      }
      r
    })
  }


  //rdd.map(kafka.send(_)).foreachPArtition(partition => slidingPrefetchIterator(partition, 1000))

}

/**
  * Implicit helper.
  * 3 parameters can be tuned:
  * - the pool size (how many thread are created),
  * - the batch size (how many tasks are executed simultaneously)
  * - the future timeout
  */
object AsynchRDDHelper {
  val CONF_POOL_SIZE = "spark.multithreading.pool.size"
  val CONF_BATCH_SIZE = "spark.multithreading.batch.size"
  val CONF_TIMEOUT = "spark.multithreading.timeout.sec"
  val CONF_SINGLE_THREADPOOL = "spark.multithreading.single.pool.per.executor"
  private val logger: Logger = LoggerFactory.getLogger(AsynchRDDHelper.getClass)

  case class AsynchConfig(poolSize: Int, batchSize: Int, timeout: Int, singleThreadPerExecutor: Boolean, poolId: String = UUID.randomUUID().toString)

  implicit class AsyncElementPartitionRDD[ROW: ClassTag](rdd: RDD[ROW]) {

    def mapPartitionAsynchWithSession[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW], Session) => Iterator[Future[RESULT]]) = {
      val conf = rdd.sparkContext.getConf
      val asynchConfig = buildAsynchConfig(conf)
      val cassandraConnector = CassandraConnector(conf)
      rdd.mapPartitions(partition => {
        val session = cassandraConnector.openSession()
        AsynchRDDProcessor.processBatch((it: AsynchPartitionIterator[ROW]) => {
          (f(it, session), () => session.close())
        }, partition, asynchConfig)
      })
    }

    def mapPartitionAsynch[RESULT: ClassTag](f: AsynchPartitionIterator[ROW] => Iterator[Future[RESULT]]) = {
      mapPartitionAsynchClosable((it: AsynchPartitionIterator[ROW]) => {
        (f(it), () => Unit)
      })
    }

    def mapPartitionAsynchClosable[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit)) = {
      val asynchConfig = buildAsynchConfig(rdd.sparkContext.getConf)
      rdd.mapPartitions(partition => {
        AsynchRDDProcessor.processBatch(f, partition, asynchConfig)
      })
    }

    def foreachPartitionAsynchClosable[RESULT: ClassTag](f: (AsynchPartitionIterator[ROW]) => (Iterator[Future[RESULT]], () => Unit)) = {
      val asynchConfig = buildAsynchConfig(rdd.sparkContext.getConf)
      rdd.foreachPartition(partition => {
        AsynchRDDProcessor.processBatch(f, partition, asynchConfig)
      })
    }
  }

  private def buildAsynchConfig(conf: SparkConf) = {
    val singleThread = conf.getOption(CONF_SINGLE_THREADPOOL).getOrElse("true").toBoolean
    AsynchConfig(getConfWithDefault(conf, CONF_POOL_SIZE, 100), getConfWithDefault(conf, CONF_BATCH_SIZE, 100), 100, singleThread, getConfWithDefault(conf, CONF_TIMEOUT).toString)
  }

  private def getConfWithDefault(conf: SparkConf, value: String, default: Int = 10): Int = {
    if (conf.contains(value)) {
      conf.get(value).toInt
    }
    else {
      logger.debug(s"spark conf $value isn't set, use $default as default")
      default
    }
  }
}
