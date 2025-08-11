const { Kafka } = require("kafkajs");
const {
	kafkaConfig,
	consumerConfig,
	producerConfig,
	testConfig,
} = require("./config/kafka");
const { v4: uuidv4 } = require("uuid");

class KafkaTestSuite {
	constructor() {
		this.kafka = new Kafka(kafkaConfig);
		this.producer = null;
		this.consumers = [];
		this.admin = null;
		this.receivedMessages = {};
	}

	// Admin client oluştur
	async getAdmin() {
		if (!this.admin) {
			this.admin = this.kafka.admin();
			await this.admin.connect();
		}
		return this.admin;
	}

	// Producer oluştur
	async getProducer() {
		if (!this.producer) {
			this.producer = this.kafka.producer(producerConfig);
			await this.producer.connect();
		}
		return this.producer;
	}

	// Consumer oluştur
	async createConsumer(groupId, consumerId) {
		const consumer = this.kafka.consumer({
			...consumerConfig,
			groupId: groupId,
		});

		await consumer.connect();
		this.consumers.push({ consumer, groupId, consumerId });
		return consumer;
	}

	// Bağlantı testi
	async testConnection() {
		console.log("🔗 Kafka bağlantısı test ediliyor...");

		try {
			const admin = await this.getAdmin();
			const metadata = await admin.fetchTopicMetadata();

			console.log("✅ Kafka bağlantısı başarılı!");
			console.log(`📊 Broker sayısı: ${kafkaConfig.brokers.length}`);
			console.log(`📋 Mevcut topic sayısı: ${metadata.topics.length}`);

			return true;
		} catch (error) {
			console.error("❌ Kafka bağlantısı başarısız:", error.message);
			return false;
		}
	}

	// Topic'leri oluştur
	async createTopics() {
		console.log("\n📋 Topic'ler oluşturuluyor...");

		const admin = await this.getAdmin();

		try {
			await admin.createTopics({
				topics: testConfig.topics.map((topic) => ({
					topic: topic,
					numPartitions: 3,
					replicationFactor: 1,
					configEntries: [
						{ name: "cleanup.policy", value: "delete" },
						{ name: "retention.ms", value: "86400000" }, // 1 gün
					],
				})),
			});

			console.log("✅ Topic'ler başarıyla oluşturuldu:");
			testConfig.topics.forEach((topic, index) => {
				console.log(`  ${index + 1}. ${topic}`);
			});
		} catch (error) {
			if (error.message.includes("already exists")) {
				console.log("ℹ️ Topic'ler zaten mevcut, devam ediliyor...");
			} else {
				throw error;
			}
		}
	}

	// Consumer'ları başlat
	async startConsumers() {
		console.log("\n👂 Consumer'lar başlatılıyor...");

		this.receivedMessages = {};

		// Her topic için mesaj sayaçlarını başlat
		testConfig.topics.forEach((topic) => {
			this.receivedMessages[topic] = {};
		});

		const consumerPromises = [];

		for (const topic of testConfig.topics) {
			// 1. Pub-Sub modeli için farklı consumer group'ları
			const pubsubConsumer1 = await this.createConsumer(
				`${testConfig.consumerGroups.pubsub}-1`,
				`pubsub-1-${topic}`
			);
			const pubsubConsumer2 = await this.createConsumer(
				`${testConfig.consumerGroups.pubsub}-2`,
				`pubsub-2-${topic}`
			);

			// 2. Queue modeli için aynı consumer group
			const queueConsumer1 = await this.createConsumer(
				testConfig.consumerGroups.queue1,
				`queue-1-${topic}`
			);
			const queueConsumer2 = await this.createConsumer(
				testConfig.consumerGroups.queue1,
				`queue-2-${topic}`
			);

			// Her consumer için dinleme başlat
			const consumers = [
				{
					consumer: pubsubConsumer1,
					id: `PubSub-1-${topic}`,
					model: "PubSub",
				},
				{
					consumer: pubsubConsumer2,
					id: `PubSub-2-${topic}`,
					model: "PubSub",
				},
				{
					consumer: queueConsumer1,
					id: `Queue-1-${topic}`,
					model: "Queue",
				},
				{
					consumer: queueConsumer2,
					id: `Queue-2-${topic}`,
					model: "Queue",
				},
			];

			for (const { consumer, id, model } of consumers) {
				await consumer.subscribe({ topic, fromBeginning: true });

				// Her consumer için mesaj sayacını başlat
				this.receivedMessages[topic][id] = [];

				consumerPromises.push(
					consumer.run({
						eachMessage: async ({
							topic: msgTopic,
							partition,
							message,
						}) => {
							const messageData = {
								key: message.key?.toString(),
								value: JSON.parse(message.value.toString()),
								partition,
								offset: message.offset,
								timestamp: new Date(
									parseInt(message.timestamp)
								),
								consumerId: id,
								model: model,
							};

							this.receivedMessages[msgTopic][id].push(
								messageData
							);

							console.log(
								`📨 [${id}] ${messageData.value.message} (P:${partition}, O:${message.offset})`
							);
						},
					})
				);
			}
		}

		console.log(`✅ ${this.consumers.length} consumer başlatıldı`);
		console.log("📡 Tüm topic'ler dinleniyor...\n");

		return consumerPromises;
	}

	// Mesajları gönder
	async sendMessages() {
		console.log("📤 Mesajlar gönderiliyor...\n");

		const producer = await this.getProducer();

		for (const topic of testConfig.topics) {
			console.log(`📋 ${topic} topic'ine mesajlar gönderiliyor...`);

			const messages = [];
			for (let i = 1; i <= testConfig.messageCount; i++) {
				messages.push({
					key: `${topic}-key-${i}`,
					value: JSON.stringify({
						id: uuidv4(),
						message: `${topic} mesajı #${i}`,
						topic: topic,
						sequence: i,
						timestamp: new Date().toISOString(),
						sender: "kafka-test-suite",
					}),
					headers: {
						"content-type": "application/json",
						topic: topic,
						sequence: i.toString(),
					},
				});
			}

			const startTime = Date.now();
			await producer.send({
				topic: topic,
				messages: messages,
			});
			const endTime = Date.now();

			console.log(
				`✅ ${topic}: ${testConfig.messageCount} mesaj gönderildi (${
					endTime - startTime
				}ms)`
			);
		}

		console.log("\n📫 Tüm mesajlar gönderildi!\n");
	}

	// Sonuçları analiz et
	async analyzeResults() {
		console.log("📊 Sonuçlar analiz ediliyor...\n");

		for (const topic of testConfig.topics) {
			console.log(`📋 ${topic.toUpperCase()} TOPIC ANALİZİ:`);
			console.log("═".repeat(50));

			const topicMessages = this.receivedMessages[topic];

			// Pub-Sub modeli analizi
			console.log("🔄 Pub-Sub Modeli (Farklı Gruplar):");
			const pubsub1 = topicMessages[`PubSub-1-${topic}`] || [];
			const pubsub2 = topicMessages[`PubSub-2-${topic}`] || [];

			console.log(`  📨 PubSub-1: ${pubsub1.length} mesaj aldı`);
			console.log(`  📨 PubSub-2: ${pubsub2.length} mesaj aldı`);
			console.log(
				`  🎯 Beklenen: Her consumer ${testConfig.messageCount} mesaj almalı`
			);

			// Queue modeli analizi
			console.log("\n📦 Queue Modeli (Aynı Grup):");
			const queue1 = topicMessages[`Queue-1-${topic}`] || [];
			const queue2 = topicMessages[`Queue-2-${topic}`] || [];

			console.log(`  📨 Queue-1: ${queue1.length} mesaj aldı`);
			console.log(`  📨 Queue-2: ${queue2.length} mesaj aldı`);
			console.log(
				`  🎯 Toplam: ${queue1.length + queue2.length} (Beklenen: ${
					testConfig.messageCount
				})`
			);

			// Partition dağılımı
			const allMessages = [...pubsub1, ...pubsub2, ...queue1, ...queue2];
			const partitionStats = {};

			allMessages.forEach((msg) => {
				if (!partitionStats[msg.partition]) {
					partitionStats[msg.partition] = 0;
				}
				partitionStats[msg.partition]++;
			});

			console.log("\n📈 Partition Dağılımı:");
			Object.keys(partitionStats).forEach((partition) => {
				console.log(
					`  Partition ${partition}: ${partitionStats[partition]} mesaj`
				);
			});

			console.log("\n");
		}

		// Genel özet
		let totalSent = testConfig.topics.length * testConfig.messageCount;
		let totalReceived = 0;

		Object.values(this.receivedMessages).forEach((topicMessages) => {
			Object.values(topicMessages).forEach((messages) => {
				totalReceived += messages.length;
			});
		});

		console.log("📊 GENEL ÖZET:");
		console.log("═".repeat(30));
		console.log(`📤 Gönderilen toplam mesaj: ${totalSent}`);
		console.log(`📥 Alınan toplam mesaj: ${totalReceived}`);
		console.log(
			`🎯 Başarı oranı: ${(
				(totalReceived / (totalSent * 4)) *
				100
			).toFixed(1)}%`
		); // 4 consumer per topic
	}

	// Temizlik
	async cleanup() {
		console.log("\n🧹 Temizlik yapılıyor...");

		const promises = [];

		// Consumer'ları kapat
		for (const { consumer } of this.consumers) {
			promises.push(consumer.disconnect());
		}

		// Producer'ı kapat
		if (this.producer) {
			promises.push(this.producer.disconnect());
		}

		// Admin'i kapat
		if (this.admin) {
			promises.push(this.admin.disconnect());
		}

		await Promise.all(promises);
		console.log("✅ Tüm bağlantılar kapatıldı");
	}

	// Ana test fonksiyonu
	async runFullTest() {
		console.log("🚀 Kafka Kapsamlı Test Başlatılıyor...");
		console.log("═".repeat(50));

		try {
			// 1. Bağlantı testi
			const isConnected = await this.testConnection();
			if (!isConnected) {
				throw new Error("Kafka bağlantısı başarısız");
			}

			// 2. Topic'leri oluştur
			await this.createTopics();

			// 3. Consumer'ları başlat
			await this.startConsumers();

			// 4. Mesajların işlenmesi için kısa bekle
			await new Promise((resolve) => setTimeout(resolve, 2000));

			// 5. Mesajları gönder
			await this.sendMessages();

			// 6. Mesajların tümünün işlenmesini bekle
			console.log("⏳ Mesajların işlenmesi bekleniyor...");
			await new Promise((resolve) => setTimeout(resolve, 5000));

			// 7. Sonuçları analiz et
			await this.analyzeResults();

			console.log("🎉 Test başarıyla tamamlandı!");
		} catch (error) {
			console.error("💥 Test sırasında hata oluştu:", error.message);
		} finally {
			await this.cleanup();
		}
	}
}

// Ana fonksiyon
async function main() {
	const testSuite = new KafkaTestSuite();

	// Graceful shutdown için
	process.on("SIGINT", async () => {
		console.log("\n👋 Uygulama sonlandırılıyor...");
		await testSuite.cleanup();
		process.exit(0);
	});

	await testSuite.runFullTest();
}

// Uygulama başlangıcı
if (require.main === module) {
	main().catch(console.error);
}
