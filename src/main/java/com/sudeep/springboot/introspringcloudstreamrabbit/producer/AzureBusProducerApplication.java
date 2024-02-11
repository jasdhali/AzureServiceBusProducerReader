package com.sudeep.springboot.introspringcloudstreamrabbit.producer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.azure.messaging.servicebus.*;
import com.azure.identity.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class AzureBusProducerApplication implements CommandLineRunner{
	static String queueName = "jas-practice-queue";
	public static void main(String[] args) {
		SpringApplication.run(AzureBusProducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		//sendMessage();	
		//sendMessageBatch();
	}	
	static void sendMessage()
	{
	    // create a token using the default Azure credential
	    DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()
	            .build();
	    //jas-azure-messaging-namespace
	    ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
	            .fullyQualifiedNamespace("jas-azure-messaging-namespace.servicebus.windows.net")
	            .credential(credential)
	            .sender()
	            .queueName(queueName)
	            .buildClient();

	    // send one message to the queue
	    senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
	    System.out.println("Sent a single message to the queue: " + queueName);
	}
	static List<ServiceBusMessage> createMessages()
	{
	    // create a list of messages and return it to the caller
	    ServiceBusMessage[] messages = {
	            new ServiceBusMessage("First message"),
	            new ServiceBusMessage("Second message"),
	            new ServiceBusMessage("Third message")
	    };
	    return Arrays.asList(messages);
	}
	static void sendMessageBatch()
	{
	    // create a token using the default Azure credential
	    DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()
	            .build();

	    ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
	            .fullyQualifiedNamespace("jas-azure-messaging-namespace.servicebus.windows.net")
	            .credential(credential)
	            .sender()
	            .queueName(queueName)
	            .buildClient();

	    // Creates an ServiceBusMessageBatch where the ServiceBus.
	    ServiceBusMessageBatch messageBatch = senderClient.createMessageBatch();

	    // create a list of messages
	    List<ServiceBusMessage> listOfMessages = createMessages();

	    // We try to add as many messages as a batch can fit based on the maximum size and send to Service Bus when
	    // the batch can hold no more messages. Create a new batch for next set of messages and repeat until all
	    // messages are sent.
	    for (ServiceBusMessage message : listOfMessages) {
	        if (messageBatch.tryAddMessage(message)) {
	            continue;
	        }

	        // The batch is full, so we create a new batch and send the batch.
	        senderClient.sendMessages(messageBatch);
	        System.out.println("Sent a batch of messages to the queue: " + queueName);

	        // create a new batch
	        messageBatch = senderClient.createMessageBatch();

	        // Add that message that we couldn't before.
	        if (!messageBatch.tryAddMessage(message)) {
	            System.err.printf("Message is too large for an empty batch. Skipping. Max size: %s.", messageBatch.getMaxSizeInBytes());
	        }
	    }

	    if (messageBatch.getCount() > 0) {
	        senderClient.sendMessages(messageBatch);
	        System.out.println("Sent a batch of messages to the queue: " + queueName);
	    }

	    //close the client
	    senderClient.close();
	}
}
