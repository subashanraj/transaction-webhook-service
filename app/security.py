import hashlib
import hmac


def mask_account(value: str) -> str:
    if len(value) <= 6:
        return "***"
    return f"{value[:5]}***{value[-3:]}"


def verify_signature(secret: str, raw_body: bytes, signature: str | None) -> bool:
    """Razorpay style: hex(HMAC_SHA256(secret, raw_body)) in X-Razorpay-Signature."""
    if not signature:
        return False
    expected = hmac.new(secret.encode(), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip())
