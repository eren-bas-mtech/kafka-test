const KafkaClient = require("../lib/kafka-client");
const { testConfig } = require("../config/kafka");

async function testTopicOperations() {
	console.log("🧪 Kafka Topic İşlemleri Testi Başlatılıyor...\n");

	const client = new KafkaClient();
	const testTopicName = `${testConfig.testTopic}-${Date.now()}`;

	try {
		// Önce bağlantıyı test et
		console.log("1️⃣ Bağlantı kontrolü...");
		const isConnected = await client.testConnection();

		if (!isConnected) {
			throw new Error("Kafka bağlantısı kurulamadı");
		}

		// Mevcut topic listesini göster
		console.log("\n2️⃣ Mevcut topic listesi:");
		let topics = await client.listTopics();
		console.log(`📋 Toplam ${topics.length} topic bulundu`);
		topics.slice(0, 5).forEach((topic, index) => {
			console.log(`  ${index + 1}. ${topic}`);
		});
		if (topics.length > 5) {
			console.log(`  ... ve ${topics.length - 5} tane daha`);
		}

		// Yeni topic oluştur
		console.log(`\n3️⃣ Yeni topic oluşturuluyor: ${testTopicName}`);
		await client.createTopic(testTopicName, 3, 1); // 3 partition, 1 replication

		// Topic listesini tekrar kontrol et
		console.log("\n4️⃣ Topic oluşturulduktan sonra liste:");
		topics = await client.listTopics();
		const isTopicCreated = topics.includes(testTopicName);

		if (isTopicCreated) {
			console.log(
				`✅ Topic '${testTopicName}' başarıyla oluşturuldu ve listede görünüyor`
			);
		} else {
			console.log(
				`⚠️ Topic '${testTopicName}' oluşturuldu ama henüz listede görünmüyor`
			);
		}

		// Topic silmeyi test et
		console.log(`\n5️⃣ Test topic'i siliniyor: ${testTopicName}`);
		await client.deleteTopic(testTopicName);

		// Final kontrol
		console.log("\n6️⃣ Topic silindikten sonra son kontrol:");
		topics = await client.listTopics();
		const isTopicDeleted = !topics.includes(testTopicName);

		if (isTopicDeleted) {
			console.log(`✅ Topic '${testTopicName}' başarıyla silindi`);
		} else {
			console.log(`⚠️ Topic '${testTopicName}' hala listede görünüyor`);
		}
	} catch (error) {
		console.error("💥 Topic testi sırasında hata oluştu:", error.message);

		// Hata durumunda da test topic'ini temizlemeye çalış
		try {
			await client.deleteTopic(testTopicName);
			console.log(`🧹 Test topic '${testTopicName}' temizlendi`);
		} catch (cleanupError) {
			console.log(`⚠️ Test topic temizlenemedi: ${cleanupError.message}`);
		}

		process.exit(1);
	} finally {
		await client.disconnect();
	}

	console.log("\n✅ Topic işlemleri testi tamamlandı!");
}

// Script direkt çalıştırılırsa testi başlat
if (require.main === module) {
	testTopicOperations().catch(console.error);
}

module.exports = testTopicOperations;
