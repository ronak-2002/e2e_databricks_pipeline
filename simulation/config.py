import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv(override=False)


@dataclass
class Config:
    # Kafka Mode: MOCK, SASL_SSL, SSL, PLAINTEXT
    KAFKA_MODE: str = os.getenv("KAFKA_MODE", "MOCK").upper()

    # Kafka Connection
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_SECURITY_PROTOCOL: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL")
    KAFKA_SASL_MECHANISM: str = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
    KAFKA_SASL_USERNAME: str = os.getenv("KAFKA_SASL_USERNAME", "")
    KAFKA_SASL_PASSWORD: str = os.getenv("KAFKA_SASL_PASSWORD", "")

    # SSL / mTLS Credentials
    KAFKA_SSL_CA_LOCATION: str = os.getenv("KAFKA_SSL_CA_LOCATION", "")
    KAFKA_SSL_CERTIFICATE_LOCATION: str = os.getenv("KAFKA_SSL_CERTIFICATE_LOCATION", "")
    KAFKA_SSL_KEY_LOCATION: str = os.getenv("KAFKA_SSL_KEY_LOCATION", "")
    KAFKA_SSL_KEY_PASSWORD: str = os.getenv("KAFKA_SSL_KEY_PASSWORD", "")
    KAFKA_ENABLE_SSL_VERIFICATION: bool = os.getenv("KAFKA_ENABLE_SSL_VERIFICATION", "true").lower() in ("true", "1", "yes")

    # Topic Names (Strictly 4 topics max)
    TOPIC_USERS: str = os.getenv("TOPIC_USERS", "ecom.users")
    TOPIC_BROWSE: str = os.getenv("TOPIC_BROWSE", "ecom.browse")
    TOPIC_CART: str = os.getenv("TOPIC_CART", "ecom.cart")
    TOPIC_ORDERS: str = os.getenv("TOPIC_ORDERS", "ecom.orders")

    # Application Settings
    APP_VERSION: str = os.getenv("APP_VERSION", "2.4.1")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "simulated-ecom-secret-key-39281")
    FLASK_PORT: int = int(os.getenv("FLASK_PORT", "5000"))
    FLASK_DEBUG: bool = os.getenv("FLASK_DEBUG", "false").lower() in ("true", "1", "yes")

    # Traffic Simulator Defaults
    SIMULATOR_NUM_USERS: int = int(os.getenv("SIMULATOR_NUM_USERS", "5"))
    SIMULATOR_TICK_SECONDS: float = float(os.getenv("SIMULATOR_TICK_SECONDS", "1.5"))

    def to_safe_dict(self) -> dict:
        """Return configuration dictionary with masked sensitive fields."""
        return {
            "kafka_mode": self.KAFKA_MODE,
            "kafka_bootstrap_servers": self.KAFKA_BOOTSTRAP_SERVERS,
            "kafka_security_protocol": self.KAFKA_SECURITY_PROTOCOL,
            "kafka_sasl_mechanism": self.KAFKA_SASL_MECHANISM,
            "kafka_sasl_username": self.KAFKA_SASL_USERNAME[:3] + "***" if self.KAFKA_SASL_USERNAME else "",
            "topics": {
                "users": self.TOPIC_USERS,
                "browse": self.TOPIC_BROWSE,
                "cart": self.TOPIC_CART,
                "orders": self.TOPIC_ORDERS,
            },
            "app_version": self.APP_VERSION,
            "simulator": {
                "num_users": self.SIMULATOR_NUM_USERS,
                "tick_seconds": self.SIMULATOR_TICK_SECONDS,
            },
        }


# Global singleton instance
config = Config()
