const KafkaClient = require("../lib/kafka-client");
const { testConfig } = require("../config/kafka");
const { v4: uuidv4 } = require("uuid");

async function testProducer() {
	console.log("🧪 Kafka Producer (Mesaj Gönderme) Testi Başlatılıyor...\n");

	const client = new KafkaClient();
	const testTopicName = `${testConfig.testTopic}-producer-${Date.now()}`;

	try {
		// Topic oluştur
		console.log(`1️⃣ Test topic'i oluşturuluyor: ${testTopicName}`);
		await client.createTopic(testTopicName, 3, 1);

		// Producer'ı başlat
		console.log("\n2️⃣ Producer bağlantısı kuruluyor...");
		const producer = client.getProducer();
		await producer.connect();
		console.log("✅ Producer bağlandı");

		// Tekil mesaj gönder
		console.log("\n3️⃣ Tekil mesaj gönderiliyor...");
		const singleMessage = {
			topic: testTopicName,
			messages: [
				{
					key: "test-key-1",
					value: JSON.stringify({
						id: uuidv4(),
						message: "Bu bir test mesajıdır",
						timestamp: new Date().toISOString(),
						type: "single",
					}),
					headers: {
						"content-type": "application/json",
						source: "aws-kafka-test",
					},
				},
			],
		};

		const singleResult = await producer.send(singleMessage);
		console.log("✅ Tekil mesaj gönderildi");
		console.log(
			`📄 Partition: ${singleResult[0].partition}, Offset: ${singleResult[0].baseOffset}`
		);

		// Toplu mesaj gönder
		console.log(
			`\n4️⃣ ${testConfig.messageCount} adet toplu mesaj gönderiliyor...`
		);
		const batchMessages = [];

		for (let i = 1; i <= testConfig.messageCount; i++) {
			batchMessages.push({
				key: `batch-key-${i}`,
				value: JSON.stringify({
					id: uuidv4(),
					message: `Toplu mesaj #${i}`,
					timestamp: new Date().toISOString(),
					type: "batch",
					sequence: i,
				}),
				headers: {
					"content-type": "application/json",
					source: "aws-kafka-test",
					"batch-id": uuidv4(),
				},
			});
		}

		const batchMessage = {
			topic: testTopicName,
			messages: batchMessages,
		};

		const startTime = Date.now();
		const batchResult = await producer.send(batchMessage);
		const endTime = Date.now();

		console.log(`✅ ${testConfig.messageCount} mesaj başarıyla gönderildi`);
		console.log(`⏱️ Gönderim süresi: ${endTime - startTime}ms`);
		console.log(
			`📊 Ortalama: ${(
				(endTime - startTime) /
				testConfig.messageCount
			).toFixed(2)}ms/mesaj`
		);

		// Partition dağılımını göster
		const partitionStats = {};
		batchResult.forEach((result) => {
			const partition = result.partition;
			if (!partitionStats[partition]) {
				partitionStats[partition] = 0;
			}
			partitionStats[partition]++;
		});

		console.log("\n📈 Partition dağılımı:");
		Object.keys(partitionStats).forEach((partition) => {
			console.log(
				`  Partition ${partition}: ${partitionStats[partition]} mesaj`
			);
		});

		// Farklı key stratejileri ile test
		console.log("\n5️⃣ Farklı key stratejileri test ediliyor...");

		const strategies = [
			{ name: "Sabit Key", key: "fixed-key" },
			{ name: "Null Key", key: null },
			{ name: "Timestamp Key", key: Date.now().toString() },
			{ name: "UUID Key", key: uuidv4() },
		];

		for (const strategy of strategies) {
			const strategyMessage = {
				topic: testTopicName,
				messages: [
					{
						key: strategy.key,
						value: JSON.stringify({
							strategy: strategy.name,
							timestamp: new Date().toISOString(),
						}),
					},
				],
			};

			const result = await producer.send(strategyMessage);
			console.log(`  ${strategy.name}: Partition ${result[0].partition}`);
		}
	} catch (error) {
		console.error(
			"💥 Producer testi sırasında hata oluştu:",
			error.message
		);
		process.exit(1);
	} finally {
		// Temizlik
		try {
			await client.deleteTopic(testTopicName);
			console.log(`\n🧹 Test topic '${testTopicName}' temizlendi`);
		} catch (cleanupError) {
			console.log(`⚠️ Test topic temizlenemedi: ${cleanupError.message}`);
		}

		await client.disconnect();
	}

	console.log("\n✅ Producer testi tamamlandı!");
}

// Script direkt çalıştırılırsa testi başlat
if (require.main === module) {
	testProducer().catch(console.error);
}

module.exports = testProducer;
