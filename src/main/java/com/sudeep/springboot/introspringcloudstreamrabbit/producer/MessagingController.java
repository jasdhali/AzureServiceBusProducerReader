package com.sudeep.springboot.introspringcloudstreamrabbit.producer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusException;
import com.azure.messaging.servicebus.ServiceBusFailureReason;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

@RestController
public class MessagingController {
	static String queueName = "jas-practice-queue";
	@Autowired
	private ServiceBusSenderClient senderClient;
	@PostMapping("/sendOne")
	public ResponseEntity<String> sendMessage() {
		senderClient.sendMessage(new ServiceBusMessage("Hello, World!"));
		return new ResponseEntity<>("Message Sent Successfully",HttpStatus.OK);
	}
	
	@PostMapping("/receiveOne")
	public ResponseEntity<String> receiceMessage() {
		try {
			receiveMessages();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new ResponseEntity<>("Message Received Successfully",HttpStatus.OK);
	}
	// handles received messages
	private void receiveMessages() throws InterruptedException
	{
	    CountDownLatch countdownLatch = new CountDownLatch(1);

	    DefaultAzureCredential credential = new DefaultAzureCredentialBuilder()
	            .build();

	    ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
	            .fullyQualifiedNamespace("jas-azure-messaging-namespace.servicebus.windows.net")
	            .credential(credential)
	            .processor()
	            .queueName(queueName)
	            .processMessage(MessagingController::processMessage)
	            .processError(context -> processError(context, countdownLatch))
	            .buildProcessorClient();

	    System.out.println("Starting the processor");
	    processorClient.start();

	    TimeUnit.SECONDS.sleep(10);
	    System.out.println("Stopping and closing the processor");
	    processorClient.close();
	}
	private static void processMessage(ServiceBusReceivedMessageContext context) {
	    ServiceBusReceivedMessage message = context.getMessage();
	    System.out.printf("Processing message. Session: %s, Sequence #: %s. Contents: %s%n", message.getMessageId(),
	        message.getSequenceNumber(), message.getBody());
	}
	private static void processError(ServiceBusErrorContext context, CountDownLatch countdownLatch) {
	    System.out.printf("Error when receiving messages from namespace: '%s'. Entity: '%s'%n",
	        context.getFullyQualifiedNamespace(), context.getEntityPath());

	    if (!(context.getException() instanceof ServiceBusException)) {
	        System.out.printf("Non-ServiceBusException occurred: %s%n", context.getException());
	        return;
	    }

	    ServiceBusException exception = (ServiceBusException) context.getException();
	    ServiceBusFailureReason reason = exception.getReason();

	    if (reason == ServiceBusFailureReason.MESSAGING_ENTITY_DISABLED
	        || reason == ServiceBusFailureReason.MESSAGING_ENTITY_NOT_FOUND
	        || reason == ServiceBusFailureReason.UNAUTHORIZED) {
	        System.out.printf("An unrecoverable error occurred. Stopping processing with reason %s: %s%n",
	            reason, exception.getMessage());

	        countdownLatch.countDown();
	    } else if (reason == ServiceBusFailureReason.MESSAGE_LOCK_LOST) {
	        System.out.printf("Message lock lost for message: %s%n", context.getException());
	    } else if (reason == ServiceBusFailureReason.SERVICE_BUSY) {
	        try {
	            // Choosing an arbitrary amount of time to wait until trying again.
	            TimeUnit.SECONDS.sleep(1);
	        } catch (InterruptedException e) {
	            System.err.println("Unable to sleep for period of time");
	        }
	    } else {
	        System.out.printf("Error source %s, reason %s, message: %s%n", context.getErrorSource(),
	            reason, context.getException());
	    }
	}
	@PostMapping("/sendBatch")
	public ResponseEntity<String> sendMessagesInBatch() {		
		return new ResponseEntity<>("Messages Batch Sent Successfully",HttpStatus.OK);
	}
}
