import io
import json
import logging
from os import getenv
from urllib.request import urlopen
from urllib.parse import urlparse

import boto3
import magic
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ALLOWED_MIME_TYPES = {
    "image/bmp":  ".bmp",
    "image/jpeg": ".jpg",
    "image/png":  ".png",
    "image/webp": ".webp",
    "video/mp4":  ".mp4",
}


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

def init_client() -> boto3.client:
    """Initialise and validate an S3 client from environment variables."""
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=getenv("aws_access_key_id"),
            aws_secret_access_key=getenv("aws_secret_access_key"),
            aws_session_token=getenv("aws_session_token"),
            region_name=getenv("aws_region_name"),
        )
        client.list_buckets()  # credential smoke-test
        logger.info("S3 client initialised successfully.")
        return client
    except ClientError as e:
        logger.error("Failed to initialise S3 client: %s", e)
        raise


# ---------------------------------------------------------------------------
# Bucket operations
# ---------------------------------------------------------------------------

def list_buckets(aws_s3_client) -> dict | bool:
    """Return the list_buckets response or False on error."""
    try:
        response = aws_s3_client.list_buckets()
        logger.info("Listed buckets successfully.")
        return response
    except ClientError as e:
        logger.error("list_buckets failed: %s", e)
        return False


def create_bucket(aws_s3_client, bucket_name: str, region: str = "us-west-2") -> bool:
    """Create a bucket in the given region. Returns True on success."""
    try:
        location = {"LocationConstraint": region}
        response = aws_s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=location,
        )
        logger.info("Bucket '%s' created.", bucket_name)
    except ClientError as e:
        logger.error("create_bucket failed: %s", e)
        return False

    return response["ResponseMetadata"]["HTTPStatusCode"] == 200


def delete_bucket(aws_s3_client, bucket_name: str) -> bool:
    """Delete an empty bucket. Returns True on success."""
    try:
        response = aws_s3_client.delete_bucket(Bucket=bucket_name)
        logger.info("Bucket '%s' deleted.", bucket_name)
    except ClientError as e:
        logger.error("delete_bucket failed: %s", e)
        return False

    return response["ResponseMetadata"]["HTTPStatusCode"] == 204


def bucket_exists(aws_s3_client, bucket_name: str) -> bool:
    """Return True if the bucket exists and is accessible."""
    try:
        aws_s3_client.head_bucket(Bucket=bucket_name)
        logger.info("Bucket '%s' exists.", bucket_name)
        return True
    except ClientError as e:
        logger.warning("bucket_exists check failed for '%s': %s", bucket_name, e)
        return False


# ---------------------------------------------------------------------------
# Policy helpers
# ---------------------------------------------------------------------------

def generate_public_read_policy(bucket_name: str) -> str:
    """Return a JSON string for a public-read bucket policy."""
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicReadGetObject",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": f"arn:aws:s3:::{bucket_name}/*",
            }
        ],
    }
    return json.dumps(policy)


def create_bucket_policy(aws_s3_client, bucket_name: str) -> None:
    """Remove the public-access block then attach a public-read policy."""
    try:
        aws_s3_client.delete_public_access_block(Bucket=bucket_name)
        aws_s3_client.put_bucket_policy(
            Bucket=bucket_name,
            Policy=generate_public_read_policy(bucket_name),
        )
        logger.info("Public-read policy applied to '%s'.", bucket_name)
    except ClientError as e:
        logger.error("create_bucket_policy failed: %s", e)
        raise


def read_bucket_policy(aws_s3_client, bucket_name: str) -> str | bool:
    """Return the bucket policy as a string, or False if none / error."""
    try:
        policy = aws_s3_client.get_bucket_policy(Bucket=bucket_name)
        policy_str = policy["Policy"]
        logger.info("Read policy for '%s'.", bucket_name)
        return policy_str
    except ClientError as e:
        logger.error("read_bucket_policy failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Object ACL
# ---------------------------------------------------------------------------

def set_object_access_policy(aws_s3_client, bucket_name: str, file_name: str) -> bool:
    """Set a single object to public-read. Returns True on success."""
    try:
        response = aws_s3_client.put_object_acl(
            ACL="public-read",
            Bucket=bucket_name,
            Key=file_name,
        )
        logger.info("ACL set to public-read for '%s/%s'.", bucket_name, file_name)
    except ClientError as e:
        logger.error("set_object_access_policy failed: %s", e)
        return False

    return response["ResponseMetadata"]["HTTPStatusCode"] == 200


# ---------------------------------------------------------------------------
# Upload with MIME validation
# ---------------------------------------------------------------------------

def _detect_mime(content: bytes) -> str:
    """Return the MIME type of raw bytes using libmagic."""
    return magic.from_buffer(content, mime=True)


def _derive_key(file_name: str, mime_type: str) -> str:
    """
    Ensure the key has the correct extension for the detected MIME type.
    Replaces whatever extension the caller supplied (or adds one if missing).
    """
    base = file_name.rsplit(".", 1)[0] if "." in file_name else file_name
    return base + ALLOWED_MIME_TYPES[mime_type]


def download_file_and_upload_to_s3(
    aws_s3_client,
    bucket_name: str,
    url: str,
    file_name: str,
    keep_local: bool = False,
) -> str:
    """
    Download *url*, validate its MIME type, then upload it to S3.

    Accepted formats: .bmp · .jpg/.jpeg · .png · .webp · .mp4

    Returns the public S3 URL on success, raises ValueError / RuntimeError on
    validation or upload failure.
    """
    logger.info("Downloading '%s' …", url)
    with urlopen(url) as response:
        content = response.read()

    # --- MIME validation ---
    mime_type = _detect_mime(content)
    if mime_type not in ALLOWED_MIME_TYPES:
        raise ValueError(
            f"Rejected file type '{mime_type}'. "
            f"Allowed: {', '.join(ALLOWED_MIME_TYPES)}"
        )

    # Correct the key extension to match actual content
    key = _derive_key(file_name, mime_type)
    logger.info("MIME OK (%s) — uploading as '%s'.", mime_type, key)

    try:
        aws_s3_client.upload_fileobj(
            Fileobj=io.BytesIO(content),
            Bucket=bucket_name,
            Key=key,
            ExtraArgs={"ContentType": mime_type},
        )
    except Exception as e:
        logger.error("Upload failed: %s", e)
        raise RuntimeError(f"Upload failed: {e}") from e

    if keep_local:
        with open(key, mode="wb") as fh:
            fh.write(content)
        logger.info("Saved local copy as '%s'.", key)

    region = getenv("aws_region_name", "us-west-2")
    public_url = f"[s3-{region}.amazonaws.com](https://s3-{region}.amazonaws.com/{bucket_name}/{key})"
    logger.info("Upload complete: %s", public_url)
    return public_url
