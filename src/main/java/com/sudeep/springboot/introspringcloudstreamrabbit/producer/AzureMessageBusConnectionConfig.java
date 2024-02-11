package com.sudeep.springboot.introspringcloudstreamrabbit.producer;

import java.util.concurrent.CountDownLatch;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusProcessorClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;

@Configuration
public class AzureMessageBusConnectionConfig {
	static String queueName = "jas-practice-queue";
	
	@Bean
	public ServiceBusSenderClient busSenderClient() {
		// create a token using the default Azure credential
		// DefaultAzureCredential credential = new
		// DefaultAzureCredentialBuilder().build();
		// jas-azure-messaging-namespace
		ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
				.fullyQualifiedNamespace("jas-azure-messaging-namespace.servicebus.windows.net")
				.credential(azureCredential()).sender().queueName(queueName).buildClient();
		return senderClient;
	}
	//@Bean
	public ServiceBusProcessorClient busReceiverClient() {
		CountDownLatch countdownLatch = new CountDownLatch(1);
		ServiceBusProcessorClient processorClient = new ServiceBusClientBuilder()
				.fullyQualifiedNamespace("jas-azure-messaging-namespace.servicebus.windows.net")
				.credential(azureCredential()).processor().queueName(queueName)
				//.processMessage(AzureMessageBusConnectionConfig::processMessage)
				//.processError(context -> processError(context, countdownLatch))
				.buildProcessorClient();
		return processorClient;
	}
	
	private DefaultAzureCredential azureCredential() {
		return new DefaultAzureCredentialBuilder().build();
	}	
}
