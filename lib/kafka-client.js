const { Kafka } = require("kafkajs");
const {
	kafkaConfig,
	consumerConfig,
	producerConfig,
} = require("../config/kafka");

class KafkaClient {
	constructor() {
		this.kafka = new Kafka(kafkaConfig);
		this.producer = null;
		this.consumer = null;
		this.admin = null;
	}

	// Admin client oluştur
	getAdmin() {
		if (!this.admin) {
			this.admin = this.kafka.admin();
		}
		return this.admin;
	}

	// Producer oluştur
	getProducer() {
		if (!this.producer) {
			this.producer = this.kafka.producer(producerConfig);
		}
		return this.producer;
	}

	// Consumer oluştur
	getConsumer(groupId = consumerConfig.groupId) {
		if (!this.consumer) {
			this.consumer = this.kafka.consumer({
				...consumerConfig,
				groupId,
			});
		}
		return this.consumer;
	}

	// Bağlantı testi
	async testConnection() {
		const admin = this.getAdmin();
		try {
			console.log("🔗 Kafka bağlantısı test ediliyor...");
			await admin.connect();
			const metadata = await admin.fetchTopicMetadata();
			console.log("✅ Kafka bağlantısı başarılı!");
			console.log(`📊 Broker sayısı: ${kafkaConfig.brokers.length}`);
			console.log(`📋 Mevcut topic sayısı: ${metadata.topics.length}`);
			return true;
		} catch (error) {
			console.error("❌ Kafka bağlantısı başarısız:", error.message);
			return false;
		} finally {
			await admin.disconnect();
		}
	}

	// Topic listesini getir
	async listTopics() {
		const admin = this.getAdmin();
		try {
			await admin.connect();
			const metadata = await admin.fetchTopicMetadata();
			return metadata.topics.map((topic) => topic.name);
		} catch (error) {
			console.error("❌ Topic listesi alınamadı:", error.message);
			throw error;
		} finally {
			await admin.disconnect();
		}
	}

	// Topic oluştur
	async createTopic(topicName, partitions = 1, replicationFactor = 1) {
		const admin = this.getAdmin();
		try {
			await admin.connect();
			await admin.createTopics({
				topics: [
					{
						topic: topicName,
						numPartitions: partitions,
						replicationFactor: replicationFactor,
						configEntries: [
							{ name: "cleanup.policy", value: "delete" },
							{ name: "retention.ms", value: "86400000" }, // 1 gün
						],
					},
				],
			});
			console.log(`✅ Topic '${topicName}' başarıyla oluşturuldu`);
			return true;
		} catch (error) {
			if (error.message.includes("already exists")) {
				console.log(`ℹ️ Topic '${topicName}' zaten mevcut`);
				return true;
			}
			console.error(
				`❌ Topic '${topicName}' oluşturulamadı:`,
				error.message
			);
			throw error;
		} finally {
			await admin.disconnect();
		}
	}

	// Topic sil
	async deleteTopic(topicName) {
		const admin = this.getAdmin();
		try {
			await admin.connect();
			await admin.deleteTopics({
				topics: [topicName],
			});
			console.log(`✅ Topic '${topicName}' başarıyla silindi`);
			return true;
		} catch (error) {
			console.error(`❌ Topic '${topicName}' silinemedi:`, error.message);
			throw error;
		} finally {
			await admin.disconnect();
		}
	}

	// Temizlik
	async disconnect() {
		const promises = [];

		if (this.producer) {
			promises.push(this.producer.disconnect());
		}

		if (this.consumer) {
			promises.push(this.consumer.disconnect());
		}

		if (this.admin) {
			promises.push(this.admin.disconnect());
		}

		await Promise.all(promises);
		console.log("🔌 Tüm Kafka bağlantıları kapatıldı");
	}
}

module.exports = KafkaClient;
