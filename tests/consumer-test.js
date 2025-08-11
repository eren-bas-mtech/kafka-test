const KafkaClient = require("../lib/kafka-client");
const { testConfig } = require("../config/kafka");
const { v4: uuidv4 } = require("uuid");

async function testConsumer() {
	console.log("🧪 Kafka Consumer (Mesaj Okuma) Testi Başlatılıyor...\n");

	const client = new KafkaClient();
	const testTopicName = `${testConfig.testTopic}-consumer-${Date.now()}`;
	let receivedMessages = [];
	let consumerRunning = false;

	try {
		// Topic oluştur
		console.log(`1️⃣ Test topic'i oluşturuluyor: ${testTopicName}`);
		await client.createTopic(testTopicName, 3, 1);

		// Önce mesajları gönder
		console.log("\n2️⃣ Test mesajları gönderiliyor...");
		const producer = client.getProducer();
		await producer.connect();

		const testMessages = [];
		for (let i = 1; i <= testConfig.messageCount; i++) {
			testMessages.push({
				key: `test-key-${i}`,
				value: JSON.stringify({
					id: uuidv4(),
					message: `Test mesajı #${i}`,
					timestamp: new Date().toISOString(),
					sequence: i,
				}),
				headers: {
					"content-type": "application/json",
					"test-id": uuidv4(),
				},
			});
		}

		await producer.send({
			topic: testTopicName,
			messages: testMessages,
		});

		console.log(`✅ ${testConfig.messageCount} test mesajı gönderildi`);

		// Consumer'ı başlat
		console.log("\n3️⃣ Consumer bağlantısı kuruluyor...");
		const consumer = client.getConsumer(`test-consumer-${Date.now()}`);
		await consumer.connect();
		console.log("✅ Consumer bağlandı");

		// Topic'e subscribe ol
		await consumer.subscribe({ topic: testTopicName, fromBeginning: true });
		console.log(`📡 Topic '${testTopicName}' dinleniyor...`);

		// Mesaj okuma işlemi
		consumerRunning = true;
		const startTime = Date.now();

		const messagePromise = new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				reject(
					new Error(
						`${testConfig.consumerTimeout}ms içinde tüm mesajlar alınamadı`
					)
				);
			}, testConfig.consumerTimeout);

			consumer.run({
				eachMessage: async ({ topic, partition, message }) => {
					try {
						const messageData = {
							partition,
							offset: message.offset,
							key: message.key?.toString(),
							value: JSON.parse(message.value.toString()),
							headers: {},
							timestamp: new Date(parseInt(message.timestamp)),
						};

						// Headers'ı parse et
						if (message.headers) {
							Object.keys(message.headers).forEach((key) => {
								messageData.headers[key] =
									message.headers[key].toString();
							});
						}

						receivedMessages.push(messageData);

						console.log(
							`📨 Mesaj alındı: ${messageData.value.message} (Partition: ${partition}, Offset: ${message.offset})`
						);

						// Tüm mesajlar alındı mı?
						if (
							receivedMessages.length >= testConfig.messageCount
						) {
							clearTimeout(timeout);
							resolve();
						}
					} catch (error) {
						console.error("❌ Mesaj işleme hatası:", error.message);
					}
				},
			});
		});

		// Mesajların gelmesini bekle
		await messagePromise;
		const endTime = Date.now();

		console.log(`\n✅ ${receivedMessages.length} mesaj başarıyla alındı`);
		console.log(`⏱️ Okuma süresi: ${endTime - startTime}ms`);
		console.log(
			`📊 Ortalama: ${(
				(endTime - startTime) /
				receivedMessages.length
			).toFixed(2)}ms/mesaj`
		);

		// Mesaj analizi
		console.log("\n4️⃣ Alınan mesajların analizi:");

		// Partition dağılımı
		const partitionStats = {};
		receivedMessages.forEach((msg) => {
			if (!partitionStats[msg.partition]) {
				partitionStats[msg.partition] = 0;
			}
			partitionStats[msg.partition]++;
		});

		console.log("📈 Partition dağılımı:");
		Object.keys(partitionStats).forEach((partition) => {
			console.log(
				`  Partition ${partition}: ${partitionStats[partition]} mesaj`
			);
		});

		// Mesaj sıralaması kontrolü
		const sequences = receivedMessages
			.map((msg) => msg.value.sequence)
			.sort((a, b) => a - b);
		const isSequential = sequences.every((seq, index) => seq === index + 1);

		console.log(
			`🔢 Mesaj sıralaması: ${isSequential ? "Düzenli" : "Karışık"}`
		);

		if (!isSequential) {
			console.log("📋 Alınan sıra:", sequences.join(", "));
		}

		// İlk ve son mesajı göster
		if (receivedMessages.length > 0) {
			const firstMsg = receivedMessages[0];
			const lastMsg = receivedMessages[receivedMessages.length - 1];

			console.log("\n📄 İlk mesaj:");
			console.log(`  Key: ${firstMsg.key}`);
			console.log(`  Sequence: ${firstMsg.value.sequence}`);
			console.log(`  Timestamp: ${firstMsg.timestamp.toISOString()}`);

			console.log("\n📄 Son mesaj:");
			console.log(`  Key: ${lastMsg.key}`);
			console.log(`  Sequence: ${lastMsg.value.sequence}`);
			console.log(`  Timestamp: ${lastMsg.timestamp.toISOString()}`);
		}

		// Consumer offset bilgisi
		console.log("\n5️⃣ Consumer grup bilgisi test ediliyor...");
		const admin = client.getAdmin();
		await admin.connect();

		try {
			const groupDescriptions = await admin.describeGroups([
				`test-consumer-${Date.now()}`,
			]);
			console.log("👥 Consumer grup durumu:");
			groupDescriptions.groups.forEach((group) => {
				console.log(`  Grup ID: ${group.groupId}`);
				console.log(`  Durum: ${group.state}`);
				console.log(`  Üye sayısı: ${group.members.length}`);
			});
		} catch (error) {
			console.log("ℹ️ Consumer grup bilgisi alınamadı (normal durum)");
		}
	} catch (error) {
		console.error(
			"💥 Consumer testi sırasında hata oluştu:",
			error.message
		);
		process.exit(1);
	} finally {
		consumerRunning = false;

		// Temizlik
		try {
			await client.deleteTopic(testTopicName);
			console.log(`\n🧹 Test topic '${testTopicName}' temizlendi`);
		} catch (cleanupError) {
			console.log(`⚠️ Test topic temizlenemedi: ${cleanupError.message}`);
		}

		await client.disconnect();
	}

	console.log("\n✅ Consumer testi tamamlandı!");
	console.log(`📊 Toplam işlenen mesaj sayısı: ${receivedMessages.length}`);
}

// Script direkt çalıştırılırsa testi başlat
if (require.main === module) {
	testConsumer().catch(console.error);
}

module.exports = testConsumer;
