package com.prisma.messagebus.queue.inmemory

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.github.matsluni.akkahttpspi.AkkaHttpClient
import com.prisma.errors.ErrorReporter
import com.prisma.messagebus.Conversions.ByteMarshaller
import com.prisma.messagebus.QueueConsumer.ConsumeFn
import com.prisma.messagebus.queue.inmemory.InMemoryQueueingMessages._
import com.prisma.messagebus.queue.{BackoffStrategy, LinearBackoff}
import com.prisma.messagebus.{ConsumerRef, Queue}
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider, EnvironmentVariableCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient

import scala.concurrent.duration._

/**
  * Queue implementation solely backed by actors, no external queueing stack is utilized.
  * Useful for the single server solution and tests.
  *
  * This is not yet a production ready implementation as robustness features for redelivery and ensuring
  * that an item is worked off are missing.
  */
case class SQSQueue[T](queueUrl: String,
                       region: Region = Region.EU_CENTRAL_1,
                       backoff: BackoffStrategy = LinearBackoff(5.seconds))(
  implicit reporter: ErrorReporter,
  marshaller: ByteMarshaller[T],
  materializer: ActorMaterializer,
  system: ActorSystem
) extends Queue[T] {

  val credentialsProvider = EnvironmentVariableCredentialsProvider.create()

  val queueClient = SqsAsyncClient
    .builder()
    .credentialsProvider(credentialsProvider)
    .region(region)
    .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
    .build()
  val router = system.actorOf(Props(RouterActor[T](backoff)))

  override def publish(msg: T): Unit = router ! Delivery(msg)

  override def shutdown: Unit = system.stop(router)

  override def withConsumer(fn: ConsumeFn[T]): ConsumerRef = {
    val worker = system.actorOf(Props(WorkerActor(router, fn)))

    router ! AddWorker(worker)
    ConsumerActorRef(worker)
  }
}

case class ConsumerActorRef(ref: ActorRef) extends ConsumerRef {
  override def stop: Unit = ref ! StopWork
}
