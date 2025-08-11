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
		console.log("📤 MESAJLAR GÖNDERİLİYOR...");
		console.log("═".repeat(40));

		const producer = await this.getProducer();

		for (const topic of testConfig.topics) {
			console.log(
				`\n📋 ${topic.toUpperCase()} topic'ine mesajlar gönderiliyor...`
			);

			const messages = [];
			for (let i = 1; i <= testConfig.messageCount; i++) {
				const messageData = {
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
				};
				messages.push(messageData);
				console.log(
					`  📝 Hazırlanan mesaj #${i}: "${topic} mesajı #${i}"`
				);
			}

			const startTime = Date.now();
			const result = await producer.send({
				topic: topic,
				messages: messages,
			});
			const endTime = Date.now();

			console.log(
				`\n  ✅ ${topic}: ${testConfig.messageCount} mesaj başarıyla gönderildi!`
			);
			console.log(`  ⏱️  Gönderim süresi: ${endTime - startTime}ms`);
			console.log(
				`  📊 Ortalama: ${(
					(endTime - startTime) /
					testConfig.messageCount
				).toFixed(2)}ms/mesaj`
			);

			// Partition dağılımını göster
			const partitionCounts = {};
			result.forEach((r) => {
				if (!partitionCounts[r.partition]) {
					partitionCounts[r.partition] = 0;
				}
				partitionCounts[r.partition]++;
			});

			console.log(`  📈 Partition dağılımı:`);
			Object.keys(partitionCounts).forEach((partition) => {
				console.log(
					`     - Partition ${partition}: ${partitionCounts[partition]} mesaj`
				);
			});
		}

		console.log("\n🎯 TÜM MESAJLAR BAŞARIYLA GÖNDERİLDİ!");
		console.log("═".repeat(40));
	}

	// Test sonuçlarını analiz et ve detaylı raporla
	async analyzeResults() {
		console.log("🧪 TEST SONUÇLARI ANALİZ EDİLİYOR...");
		console.log("═".repeat(60));

		let allTestsPassed = true;
		const testResults = {
			topics: {},
			summary: {
				totalSent: testConfig.topics.length * testConfig.messageCount,
				totalReceived: 0,
				passedTests: 0,
				failedTests: 0,
			},
		};

		// Her topic için detaylı analiz
		for (const topic of testConfig.topics) {
			console.log(`\n📋 ${topic.toUpperCase()} TOPIC TEST SONUÇLARI:`);
			console.log("─".repeat(50));

			const topicMessages = this.receivedMessages[topic];
			const topicResult = {
				sent: testConfig.messageCount,
				pubsub: { consumer1: 0, consumer2: 0, passed: false },
				queue: { consumer1: 0, consumer2: 0, total: 0, passed: false },
				partitions: {},
				messages: [],
			};

			// Mesaj sayılarını topla
			const pubsub1 = topicMessages[`PubSub-1-${topic}`] || [];
			const pubsub2 = topicMessages[`PubSub-2-${topic}`] || [];
			const queue1 = topicMessages[`Queue-1-${topic}`] || [];
			const queue2 = topicMessages[`Queue-2-${topic}`] || [];

			topicResult.pubsub.consumer1 = pubsub1.length;
			topicResult.pubsub.consumer2 = pubsub2.length;
			topicResult.queue.consumer1 = queue1.length;
			topicResult.queue.consumer2 = queue2.length;
			topicResult.queue.total = queue1.length + queue2.length;

			// Tüm mesajları birleştir ve analiz et
			const allMessages = [...pubsub1, ...pubsub2, ...queue1, ...queue2];
			testResults.summary.totalReceived += allMessages.length;

			// Her gönderilen mesajın alınıp alınmadığını kontrol et
			console.log("\n🔍 MESAJ BAZLI TEST SONUÇLARI:");
			for (let i = 1; i <= testConfig.messageCount; i++) {
				const messageText = `${topic} mesajı #${i}`;

				// Bu mesajı alan consumer'ları bul
				const pubsub1Received = pubsub1.find(
					(msg) => msg.value.sequence === i
				);
				const pubsub2Received = pubsub2.find(
					(msg) => msg.value.sequence === i
				);
				const queue1Received = queue1.find(
					(msg) => msg.value.sequence === i
				);
				const queue2Received = queue2.find(
					(msg) => msg.value.sequence === i
				);

				const queueReceived = queue1Received || queue2Received;

				// Pub-Sub testi (her consumer almalı)
				const pubsubPassed = pubsub1Received && pubsub2Received;
				// Queue testi (en az bir consumer almalı)
				const queuePassed = queueReceived;

				const overallPassed = pubsubPassed && queuePassed;

				if (overallPassed) {
					console.log(`  ✅ Mesaj #${i}: PASSED`);
					console.log(
						`     🔄 PubSub: Consumer1(P:${pubsub1Received.partition}) + Consumer2(P:${pubsub2Received.partition})`
					);
					console.log(
						`     📦 Queue: ${queueReceived.consumerId}(P:${queueReceived.partition})`
					);
					testResults.summary.passedTests++;
				} else {
					console.log(`  ❌ Mesaj #${i}: FAILED`);
					console.log(
						`     🔄 PubSub: ${
							pubsub1Received ? "✅" : "❌"
						} Consumer1, ${pubsub2Received ? "✅" : "❌"} Consumer2`
					);
					console.log(
						`     📦 Queue: ${queueReceived ? "✅" : "❌"} ${
							queueReceived
								? queueReceived.consumerId
								: "Hiç alınmadı"
						}`
					);
					testResults.summary.failedTests++;
					allTestsPassed = false;
				}

				// Partition istatistikleri
				allMessages.forEach((msg) => {
					if (msg.value.sequence === i) {
						if (!topicResult.partitions[msg.partition]) {
							topicResult.partitions[msg.partition] = 0;
						}
						topicResult.partitions[msg.partition]++;
					}
				});
			}

			// Topic bazlı genel değerlendirme
			topicResult.pubsub.passed =
				pubsub1.length === testConfig.messageCount &&
				pubsub2.length === testConfig.messageCount;
			topicResult.queue.passed =
				topicResult.queue.total === testConfig.messageCount;

			console.log("\n📊 TOPIC ÖZET:");
			console.log(
				`  🔄 PubSub Modeli: ${
					topicResult.pubsub.passed ? "✅ PASSED" : "❌ FAILED"
				}`
			);
			console.log(
				`     - Consumer1: ${topicResult.pubsub.consumer1}/${testConfig.messageCount}`
			);
			console.log(
				`     - Consumer2: ${topicResult.pubsub.consumer2}/${testConfig.messageCount}`
			);

			console.log(
				`  📦 Queue Modeli: ${
					topicResult.queue.passed ? "✅ PASSED" : "❌ FAILED"
				}`
			);
			console.log(`     - Consumer1: ${topicResult.queue.consumer1}`);
			console.log(`     - Consumer2: ${topicResult.queue.consumer2}`);
			console.log(
				`     - Toplam: ${topicResult.queue.total}/${testConfig.messageCount}`
			);

			console.log(`  📈 Partition Dağılımı:`);
			Object.keys(topicResult.partitions).forEach((partition) => {
				console.log(
					`     - Partition ${partition}: ${topicResult.partitions[partition]} mesaj`
				);
			});

			testResults.topics[topic] = topicResult;
		}

		// GENEL TEST SONUCU
		console.log("\n" + "🎯".repeat(20));
		console.log("🎯 GENEL TEST SONUCU");
		console.log("🎯".repeat(20));

		console.log(
			`📤 Toplam gönderilen mesaj: ${testResults.summary.totalSent}`
		);
		console.log(
			`📥 Toplam alınan mesaj: ${testResults.summary.totalReceived}`
		);
		console.log(`✅ Başarılı testler: ${testResults.summary.passedTests}`);
		console.log(`❌ Başarısız testler: ${testResults.summary.failedTests}`);

		const successRate = (
			(testResults.summary.passedTests /
				(testResults.summary.passedTests +
					testResults.summary.failedTests)) *
			100
		).toFixed(1);
		console.log(`📊 Başarı oranı: ${successRate}%`);

		// Model bazlı özet
		let pubsubSuccess = 0,
			queueSuccess = 0;
		Object.values(testResults.topics).forEach((topic) => {
			if (topic.pubsub.passed) pubsubSuccess++;
			if (topic.queue.passed) queueSuccess++;
		});

		console.log("\n📋 MODEL BAZLI BAŞARI ORANLARI:");
		console.log(
			`🔄 Pub-Sub Modeli: ${pubsubSuccess}/${testConfig.topics.length} topic başarılı`
		);
		console.log(
			`📦 Queue Modeli: ${queueSuccess}/${testConfig.topics.length} topic başarılı`
		);

		// Final durumu
		if (allTestsPassed) {
			console.log("\n🎉🎉🎉 TÜM TESTLER BAŞARIYLA GEÇİLDİ! 🎉🎉🎉");
		} else {
			console.log("\n⚠️⚠️⚠️ BAZI TESTLER BAŞARISIZ! ⚠️⚠️⚠️");
		}

		return testResults;
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
