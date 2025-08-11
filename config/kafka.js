require("dotenv").config();

const kafkaConfig = {
	clientId: process.env.KAFKA_CLIENT_ID || "kafka-test-client",
	brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
	connectionTimeout: 3000,
	requestTimeout: 30000,
	ssl: process.env.KAFKA_SSL === "true",
	// Username/Password authentication için
	sasl: process.env.KAFKA_USERNAME
		? {
				mechanism: "plain",
				username: process.env.KAFKA_USERNAME,
				password: process.env.KAFKA_PASSWORD,
		  }
		: undefined,
};

const consumerConfig = {
	groupId: process.env.KAFKA_GROUP_ID || "test-consumer-group",
	sessionTimeout: 30000,
	heartbeatInterval: 3000,
	maxWaitTimeInMs: 5000,
	maxBytes: 1048576,
	allowAutoTopicCreation: false,
};

const producerConfig = {
	maxInFlightRequests: 1,
	idempotent: true,
	transactionTimeout: 30000,
};

const testConfig = {
	topics: ["user-events", "order-processing", "notification-queue"],
	messageCount: parseInt(process.env.MESSAGE_COUNT) || 5,
	consumerTimeout: parseInt(process.env.CONSUMER_TIMEOUT) || 30000,
	// Consumer grupları - farklı modelleri test etmek için
	consumerGroups: {
		pubsub: "pubsub-group", // Pub-Sub modeli için (farklı gruplar)
		queue1: "queue-group-1", // Queue modeli için (aynı grup)
		queue2: "queue-group-2", // Queue modeli için (aynı grup)
	},
};

module.exports = {
	kafkaConfig,
	consumerConfig,
	producerConfig,
	testConfig,
};
