package com.prisma.messagebus.queue.sqs

import akka.stream.alpakka.sqs.javadsl.SqsSource
import akka.stream.javadsl.{Sink, Source}
import com.prisma.messagebus.Conversions.ByteMarshaller
import com.prisma.messagebus.QueuePublisher
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.SendMessageRequest

class SQSQueuePublisher[T](awsAsyncClient: SqsAsyncClient, onShutdown: () => Unit = () => {})(implicit val marshaller: ByteMarshaller[T]) extends QueuePublisher[T]{
  override def publish(msg: T): Unit = SqsSource.create(queueUrl)
    .single(SendMessageRequest.builder().messageBody(marshaller(msg)))
    .via(sink)
    .runWith(Sink.head)
  
  override def shutdown: Unit = onShutdown()
}
