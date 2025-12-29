# zs3

S3-compatible storage in 1100 lines of Zig. Zero dependencies.

I got mass-evicted by MinIO hitting 12GB RAM for storing test fixtures. So I wrote my own.

## Why

Most S3 usage is PUT, GET, DELETE, LIST with basic auth. You don't need 200k lines of code for that.

| | zs3 | MinIO | LocalStack |
|---|-----|-------|------------|
| Lines | 1,100 | 200,000 | ??? |
| Binary | 250KB | 100MB | Docker + JVM |
| RAM idle | 2MB | 200MB+ | 500MB+ |
| Dependencies | 0 | many | many |

## What it does

- Full AWS SigV4 authentication (works with aws-cli, boto3, any SDK)
- PUT, GET, DELETE, HEAD, LIST (v2)
- Multipart uploads for large files
- Range requests for streaming/seeking
- ~250KB static binary

## What it doesn't do

- Versioning, lifecycle policies, bucket ACLs
- Pre-signed URLs, object tagging, encryption
- Anything you'd actually need a cloud provider for

If you need these, use MinIO or AWS.

## Quick Start

```bash
zig build -Doptimize=ReleaseFast
./zig-out/bin/zs3
```

Server listens on port 9000, stores data in `./data`.

## Usage

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://mybucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://mybucket/ --recursive
aws --endpoint-url http://localhost:9000 s3 cp s3://mybucket/file.txt ./
aws --endpoint-url http://localhost:9000 s3 rm s3://mybucket/file.txt
```

Works with any S3 SDK:

```python
import boto3

s3 = boto3.client('s3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

s3.create_bucket(Bucket='test')
s3.put_object(Bucket='test', Key='hello.txt', Body=b'world')
print(s3.get_object(Bucket='test', Key='hello.txt')['Body'].read())
```

## The interesting bits

**SigV4 is elegant.** The whole auth flow is ~150 lines. AWS's "complex" signature scheme is really just: canonical request -> string to sign -> HMAC chain -> compare. No magic.

**Storage is just files.** `mybucket/folder/file.txt` is literally `./data/mybucket/folder/file.txt`. You can `ls` your buckets. You can `cp` files in. It just works.

**Zig makes this easy.** No runtime, no GC, no hidden allocations, no surprise dependencies. The binary is just the code + syscalls.

## When to use this

- Local dev (replacing localstack/minio)
- CI artifact storage
- Self-hosted backups
- Embedded/appliance storage
- Learning how S3 actually works

## When NOT to use this

- Production with untrusted users
- Anything requiring durability guarantees beyond "it's on your disk"
- If you need any feature in the "not supported" list

## Configuration

Edit `main.zig`:

```zig
const ctx = S3Context{
    .allocator = allocator,
    .data_dir = "data",
    .access_key = "minioadmin",
    .secret_key = "minioadmin",
};

const address = net.Address.parseIp4("0.0.0.0", 9000)
```

## Building

Requires Zig 0.15+.

```bash
zig build                                    # debug
zig build -Doptimize=ReleaseFast             # release (~250KB)
zig build -Dtarget=x86_64-linux-musl         # cross-compile
zig build test                               # run tests
```

## Testing

```bash
python3 test_client.py   # 20 integration tests
zig build test           # 11 unit tests
```

## Limits

| Limit | Value |
|-------|-------|
| Max header size | 8 KB |
| Max body size | 5 GB |
| Max key length | 1024 bytes |
| Bucket name | 3-63 chars |

## Security

- Full SigV4 signature verification
- Input validation on bucket names and object keys
- Request size limits
- No shell commands, no eval, no external network calls
- Single file, easy to audit

TLS not included. Use a reverse proxy (nginx, caddy) for HTTPS.

## License

[WTFPL](LICENSE) - Read it, fork it, break it.
