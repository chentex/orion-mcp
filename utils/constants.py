"""Shared constants for orion-mcp."""

# AES-256-GCM parameters for symmetric decryption of header payloads
AES_GCM_KEY_LENGTH_BYTES = 32
AES_GCM_NONCE_LENGTH_BYTES = 12


ORION_CONFIGS_PATH = "/orion/examples/"

RELEASE_DATES = {
    "4.17": "2024-10-29",
    "4.18": "2025-02-28",
    "4.19": "2025-06-17",
    "4.20": "2025-10-23",
    "4.21": "2026-02-25",
    "4.22": "2026-06-17",
    "5.0": "2026-10-31",
}