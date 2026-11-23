import json
import logging
import os
import threading
from typing import Any, Callable, Dict, Optional

from config import config
from state_store import state_store

# Configure logger
logger = logging.getLogger("KafkaProducerClient")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [KafkaProducer] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


class KafkaProducerClient:
    """
    Modular Apache Kafka Producer supporting:
    - MOCK mode: In-memory simulation, records to rolling event buffer without Kafka broker
    - SASL_SSL mode: Production-ready authenticated connection (e.g., Confluent Cloud, AWS MSK)
    - SSL mode: Mutual TLS (mTLS) client certificate authentication
    - PLAINTEXT mode: Local unsecured development Kafka cluster
    """

    def __init__(self, mode: Optional[str] = None):
        self.mode = (mode or config.KAFKA_MODE).upper()
        self._producer = None
        self._lock = threading.Lock()
        self._total_sent = 0
        self._total_failed = 0
        self._mock_sent = 0
        self._init_error = None

        logger.info(f"Initializing KafkaProducerClient in '{self.mode}' mode...")

        if self.mode != "MOCK":
            self._init_real_producer()

    def _init_real_producer(self):
        """Initialize confluent-kafka Producer with SASL/SSL credentials."""
        try:
            from confluent_kafka import Producer

            kafka_conf: Dict[str, Any] = {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "ecom-sim-producer",
                "security.protocol": config.KAFKA_SECURITY_PROTOCOL,
                "linger.ms": 10,
                "acks": "all",
            }

            # SASL Authentication
            if "SASL" in config.KAFKA_SECURITY_PROTOCOL.upper():
                kafka_conf["sasl.mechanism"] = config.KAFKA_SASL_MECHANISM
                if config.KAFKA_SASL_USERNAME:
                    kafka_conf["sasl.username"] = config.KAFKA_SASL_USERNAME
                if config.KAFKA_SASL_PASSWORD:
                    kafka_conf["sasl.password"] = config.KAFKA_SASL_PASSWORD

            # SSL / TLS Certificate Validation
            if not config.KAFKA_ENABLE_SSL_VERIFICATION:
                kafka_conf["enable.ssl.certificate.verification"] = False
            elif config.KAFKA_SSL_CA_LOCATION:
                kafka_conf["ssl.ca.location"] = config.KAFKA_SSL_CA_LOCATION
            elif os.path.exists("ca.pem"):
                kafka_conf["ssl.ca.location"] = "ca.pem"
            elif os.path.exists(os.path.join(os.path.dirname(__file__), "ca.pem")):
                kafka_conf["ssl.ca.location"] = os.path.join(os.path.dirname(__file__), "ca.pem")

            # Client mTLS Credentials (if applicable)
            if config.KAFKA_SSL_CERTIFICATE_LOCATION:
                kafka_conf["ssl.certificate.location"] = config.KAFKA_SSL_CERTIFICATE_LOCATION
            if config.KAFKA_SSL_KEY_LOCATION:
                kafka_conf["ssl.key.location"] = config.KAFKA_SSL_KEY_LOCATION
            if config.KAFKA_SSL_KEY_PASSWORD:
                kafka_conf["ssl.key.password"] = config.KAFKA_SSL_KEY_PASSWORD

            self._producer = Producer(kafka_conf)
            logger.info(f"Confluent Kafka Producer initialized for {config.KAFKA_BOOTSTRAP_SERVERS}")

        except Exception as e:
            self._init_error = str(e)
            logger.warning(
                f"Failed to initialize real Kafka producer ({e}). Falling back to MOCK mode."
            )
            self.mode = "MOCK"

    def send(
        self,
        topic: str,
        event: dict,
        key: Optional[str] = None,
        callback: Optional[Callable] = None,
    ) -> bool:
        """
        Emit a validated event envelope to the given topic.
        Always records the event in state_store for the live web stream inspector.
        """
        partition_key = key or event.get("user_id") or event.get("session_id", "default_key")

        # Always record in state store for live web UI stream inspector & metrics
        state_store.record_event(envelope=event, topic=topic)

        if self.mode == "MOCK":
            with self._lock:
                self._total_sent += 1
                self._mock_sent += 1
            logger.debug(f"[MOCK] Topic: {topic} | Key: {partition_key} | Event: {event.get('event_type')}")
            if callback:
                callback(None, f"MOCK_DELIVERED_{topic}")
            return True

        # Real Kafka mode (SASL_SSL / SSL / PLAINTEXT)
        if self._producer is None:
            logger.error("Producer is None in non-mock mode.")
            with self._lock:
                self._total_failed += 1
            return False

        try:
            payload_bytes = json.dumps(event).encode("utf-8")
            key_bytes = str(partition_key).encode("utf-8")

            def _delivery_report(err, msg):
                with self._lock:
                    if err is not None:
                        self._total_failed += 1
                        logger.error(f"Kafka delivery failed for topic {topic}: {err}")
                    else:
                        self._total_sent += 1
                if callback:
                    callback(err, msg)

            self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=payload_bytes,
                on_delivery=_delivery_report,
            )
            # Serve delivery reports from previous produce calls
            self._producer.poll(0)
            return True

        except Exception as ex:
            logger.error(f"Error producing event to Kafka: {ex}")
            with self._lock:
                self._total_failed += 1
            return False

    def flush(self, timeout: float = 1.0):
        """Wait for outstanding messages to be delivered."""
        if self._producer:
            self._producer.flush(timeout)

    def get_status(self) -> dict:
        """Return status and telemetry metrics."""
        with self._lock:
            return {
                "mode": self.mode,
                "bootstrap_servers": config.KAFKA_BOOTSTRAP_SERVERS if self.mode != "MOCK" else "N/A (Mock)",
                "security_protocol": config.KAFKA_SECURITY_PROTOCOL if self.mode != "MOCK" else "N/A",
                "total_sent": self._total_sent,
                "mock_sent": self._mock_sent,
                "total_failed": self._total_failed,
                "init_error": self._init_error,
                "is_mock": self.mode == "MOCK",
            }


# Global singleton producer
kafka_producer = KafkaProducerClient()
