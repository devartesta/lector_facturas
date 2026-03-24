from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from lector_facturas.google_drive import DriveConfig, GoogleOAuthConfig
from lector_facturas.payment_fees import PayPalConfig, ShopifyPaymentsConfig
from lector_facturas.review_notifications import GmailConfig


@dataclass(frozen=True)
class AppSettings:
    gmail_sender: str = ""
    gmail_recipients: tuple[str, ...] = ()
    google_client_id: str = ""
    google_client_secret: str = ""
    google_refresh_token: str = ""
    gmail_user_id: str = "me"
    drive_shared_drive_id: str = ""
    drive_root_folder_id: str = ""
    shopify_shop_name: str = ""
    shopify_client_id: str = ""
    shopify_client_secret: str = ""
    shopify_api_version: str = "2026-01"
    paypal_client_id: str = ""
    paypal_client_secret: str = ""
    paypal_base_url: str = "https://api-m.paypal.com"
    company_name: str = ""

    @property
    def gmail_ready(self) -> bool:
        return bool(
            self.gmail_sender
            and self.gmail_recipients
            and self.google_client_id
            and self.google_client_secret
            and self.google_refresh_token
        )

    def to_gmail_config(self) -> GmailConfig:
        if not self.gmail_ready:
            raise RuntimeError("Gmail settings are incomplete.")
        return GmailConfig(
            client_id=self.google_client_id,
            client_secret=self.google_client_secret,
            refresh_token=self.google_refresh_token,
            sender=self.gmail_sender,
            recipients=self.gmail_recipients,
            user_id=self.gmail_user_id,
        )

    @property
    def google_oauth_ready(self) -> bool:
        return bool(
            self.google_client_id
            and self.google_client_secret
            and self.google_refresh_token
        )

    def to_drive_config(self) -> DriveConfig:
        if not self.google_oauth_ready:
            raise RuntimeError("Google OAuth settings are incomplete.")
        return DriveConfig(
            oauth=GoogleOAuthConfig(
                client_id=self.google_client_id,
                client_secret=self.google_client_secret,
                refresh_token=self.google_refresh_token,
            ),
            shared_drive_id=self.drive_shared_drive_id,
            root_folder_id=self.drive_root_folder_id,
        )

    @property
    def shopify_ready(self) -> bool:
        return bool(self.shopify_shop_name and self.shopify_client_id and self.shopify_client_secret)

    def to_shopify_config(self) -> ShopifyPaymentsConfig:
        if not self.shopify_ready:
            raise RuntimeError("Shopify settings are incomplete.")
        return ShopifyPaymentsConfig(
            shop_name=self.shopify_shop_name,
            client_id=self.shopify_client_id,
            client_secret=self.shopify_client_secret,
            api_version=self.shopify_api_version,
        )

    @property
    def paypal_ready(self) -> bool:
        return bool(self.paypal_client_id and self.paypal_client_secret)

    def to_paypal_config(self) -> PayPalConfig:
        if not self.paypal_ready:
            raise RuntimeError("PayPal settings are incomplete.")
        return PayPalConfig(
            client_id=self.paypal_client_id,
            client_secret=self.paypal_client_secret,
            base_url=self.paypal_base_url,
        )


def load_settings() -> AppSettings:
    _load_dotenv_file(".env.local")
    _load_dotenv_file(".env.playground.local")
    recipients_env = os.environ.get("GMAIL_RECIPIENTS") or os.environ.get("GMAIL_RECIPIENT", "")
    recipients = tuple(part.strip() for part in recipients_env.split(",") if part.strip())
    return AppSettings(
        gmail_sender=os.environ.get("GMAIL_SENDER", ""),
        gmail_recipients=recipients,
        google_client_id=os.environ.get("GOOGLE_CLIENT_ID", ""),
        google_client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        google_refresh_token=os.environ.get("GOOGLE_REFRESH_TOKEN", ""),
        gmail_user_id=os.environ.get("GMAIL_USER_ID", "me"),
        drive_shared_drive_id=os.environ.get("GOOGLE_DRIVE_SHARED_DRIVE_ID", ""),
        drive_root_folder_id=os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID", ""),
        shopify_shop_name=os.environ.get("SHOPIFY_SHOP", "") or os.environ.get("SHOPIFY_SHOP_DOMAIN", ""),
        shopify_client_id=os.environ.get("SHOPIFY_CLIENT_ID", ""),
        shopify_client_secret=os.environ.get("SHOPIFY_CLIENT_SECRET", ""),
        shopify_api_version=os.environ.get("SHOPIFY_API_VERSION", "2026-01"),
        paypal_client_id=os.environ.get("PAYPAL_CLIENT_ID", ""),
        paypal_client_secret=os.environ.get("PAYPAL_CLIENT_SECRET", ""),
        paypal_base_url=os.environ.get("PAYPAL_BASE_URL", "https://api-m.paypal.com"),
        company_name=os.environ.get("COMPANY_NAME", ""),
    )


def _load_dotenv_file(filename: str) -> None:
    path = Path(__file__).resolve().parents[2] / filename
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value
