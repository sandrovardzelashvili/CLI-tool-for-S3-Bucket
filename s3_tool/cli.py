import logging
import sys

import click

from .s3 import (
    init_client,
    list_buckets,
    create_bucket,
    delete_bucket,
    bucket_exists,
    create_bucket_policy,
    read_bucket_policy,
    set_object_access_policy,
    download_file_and_upload_to_s3,
)

# ---------------------------------------------------------------------------
# Logging setup — controlled by --verbose flag
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        stream=sys.stderr,
    )


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------

@click.group()
@click.option("--verbose", "-v", is_flag=True, default=False, help="Enable debug logging.")
@click.pass_context
def cli(ctx: click.Context, verbose: bool) -> None:
    """🪣  S3 CLI Tool — manage buckets and objects from the terminal."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["client"] = init_client()


# ---------------------------------------------------------------------------
# Bucket commands
# ---------------------------------------------------------------------------

@cli.command("list-buckets")
@click.pass_context
def cmd_list_buckets(ctx: click.Context) -> None:
    """List all S3 buckets in your account."""
    result = list_buckets(ctx.obj["client"])
    if not result:
        click.echo("Failed to list buckets.", err=True)
        sys.exit(1)
    buckets = result.get("Buckets", [])
    if not buckets:
        click.echo("No buckets found.")
        return
    click.echo(f"{'Bucket Name':<50}  {'Creation Date'}")
    click.echo("-" * 70)
    for b in buckets:
        click.echo(f"{b['Name']:<50}  {b['CreationDate'].strftime('%Y-%m-%d %H:%M:%S')}")


@cli.command("create-bucket")
@click.argument("bucket_name")
@click.option("--region", default="us-west-2", show_default=True, help="AWS region.")
@click.pass_context
def cmd_create_bucket(ctx: click.Context, bucket_name: str, region: str) -> None:
    """Create a new S3 bucket."""
    ok = create_bucket(ctx.obj["client"], bucket_name, region)
    if ok:
        click.echo(f"✅  Bucket '{bucket_name}' created in {region}.")
    else:
        click.echo(f"❌  Failed to create bucket '{bucket_name}'.", err=True)
        sys.exit(1)


@cli.command("delete-bucket")
@click.argument("bucket_name")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt.")
@click.pass_context
def cmd_delete_bucket(ctx: click.Context, bucket_name: str, yes: bool) -> None:
    """Delete an S3 bucket (must be empty)."""
    if not yes:
        click.confirm(f"Delete bucket '{bucket_name}'?", abort=True)
    ok = delete_bucket(ctx.obj["client"], bucket_name)
    if ok:
        click.echo(f"✅  Bucket '{bucket_name}' deleted.")
    else:
        click.echo(f"❌  Failed to delete bucket '{bucket_name}'.", err=True)
        sys.exit(1)


@cli.command("bucket-exists")
@click.argument("bucket_name")
@click.pass_context
def cmd_bucket_exists(ctx: click.Context, bucket_name: str) -> None:
    """Check whether a bucket exists and is accessible."""
    if bucket_exists(ctx.obj["client"], bucket_name):
        click.echo(f"✅  Bucket '{bucket_name}' exists.")
    else:
        click.echo(f"❌  Bucket '{bucket_name}' does not exist or is not accessible.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Policy commands
# ---------------------------------------------------------------------------

@cli.command("create-policy")
@click.argument("bucket_name")
@click.pass_context
def cmd_create_policy(ctx: click.Context, bucket_name: str) -> None:
    """Attach a public-read bucket policy (removes public-access block first)."""
    try:
        create_bucket_policy(ctx.obj["client"], bucket_name)
        click.echo(f"✅  Public-read policy applied to '{bucket_name}'.")
    except Exception as e:
        click.echo(f"❌  {e}", err=True)
        sys.exit(1)


@cli.command("read-policy")
@click.argument("bucket_name")
@click.pass_context
def cmd_read_policy(ctx: click.Context, bucket_name: str) -> None:
    """Print the current bucket policy as JSON."""
    policy = read_bucket_policy(ctx.obj["client"], bucket_name)
    if policy:
        import json
        click.echo(json.dumps(json.loads(policy), indent=2))
    else:
        click.echo(f"No policy found for '{bucket_name}'.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Object commands
# ---------------------------------------------------------------------------

@cli.command("set-acl")
@click.argument("bucket_name")
@click.argument("file_name")
@click.pass_context
def cmd_set_acl(ctx: click.Context, bucket_name: str, file_name: str) -> None:
    """Set a specific object to public-read ACL."""
    ok = set_object_access_policy(ctx.obj["client"], bucket_name, file_name)
    if ok:
        click.echo(f"✅  ACL set to public-read for '{bucket_name}/{file_name}'.")
    else:
        click.echo("❌  Failed to set ACL.", err=True)
        sys.exit(1)


@cli.command("upload-url")
@click.argument("bucket_name")
@click.argument("url")
@click.argument("file_name")
@click.option("--keep-local", is_flag=True, default=False, help="Save a local copy.")
@click.pass_context
def cmd_upload_url(
    ctx: click.Context,
    bucket_name: str,
    url: str,
    file_name: str,
    keep_local: bool,
) -> None:
    """
    Download a file from URL and upload it to S3.

    Only .bmp, .jpg, .jpeg, .png, .webp, and .mp4 files are accepted.
    The extension is auto-corrected to match the actual MIME type.
    """
    try:
        public_url = download_file_and_upload_to_s3(
            ctx.obj["client"], bucket_name, url, file_name, keep_local
        )
        click.echo(f"✅  Uploaded successfully.\n🔗  {public_url}")
    except ValueError as e:
        click.echo(f"❌  Validation error: {e}", err=True)
        sys.exit(1)
    except RuntimeError as e:
        click.echo(f"❌  Upload error: {e}", err=True)
        sys.exit(1)
