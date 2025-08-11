const KafkaClient = require("../lib/kafka-client");

async function testConnection() {
	console.log("🧪 Kafka Bağlantı Testi Başlatılıyor...\n");

	const client = new KafkaClient();

	try {
		// Bağlantı testi
		const isConnected = await client.testConnection();

		if (isConnected) {
			console.log("\n📋 Mevcut topic listesi:");
			const topics = await client.listTopics();

			if (topics.length > 0) {
				topics.forEach((topic, index) => {
					console.log(`  ${index + 1}. ${topic}`);
				});
			} else {
				console.log("  Henüz hiç topic oluşturulmamış");
			}
		}
	} catch (error) {
		console.error("💥 Test sırasında hata oluştu:", error.message);
		process.exit(1);
	} finally {
		await client.disconnect();
	}

	console.log("\n✅ Bağlantı testi tamamlandı!");
}

// Script direkt çalıştırılırsa testi başlat
if (require.main === module) {
	testConnection().catch(console.error);
}

module.exports = testConnection;
