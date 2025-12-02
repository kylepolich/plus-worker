"""
Entry point for plus-worker Fargate tasks.

Reads configuration from environment variables and executes the requested action.
"""
import os
import sys
import time

# Required for all modes
REQUIRED_ENV = [
    'RUN_MODE',
    'JOB_ID',
    'USERNAME',
    'ACCESS_KEY',
    'SECRET_KEY',
    'DYNAMO_TABLE',
    'DYNAMO_STREAMS_TABLE',
    'PRIMARY_BUCKET',
]

# Additional required vars per mode
MODE_REQUIRED_ENV = {
    'RUN_ACTION': ['ACTION_ID'],
    'RUN_SCRIPT': ['SCRIPT_OBJECT_ID'],
}


def check_env():
    """Validate all required environment variables are set. Exit 1 if any missing."""
    missing = []

    for var in REQUIRED_ENV:
        if not os.environ.get(var):
            missing.append(var)

    run_mode = os.environ.get('RUN_MODE')
    if run_mode in MODE_REQUIRED_ENV:
        for var in MODE_REQUIRED_ENV[run_mode]:
            if not os.environ.get(var):
                missing.append(var)

    if missing:
        print("ERROR: Missing required environment variables:", file=sys.stderr)
        for var in missing:
            print(f"  - {var}", file=sys.stderr)
        sys.exit(1)

    print("Environment check passed. Found all required variables:")
    for var in REQUIRED_ENV:
        val = os.environ.get(var)
        # Mask secrets
        if 'KEY' in var or 'SECRET' in var:
            val = val[:4] + '...' if val else None
        print(f"  {var}={val}")

    if run_mode in MODE_REQUIRED_ENV:
        for var in MODE_REQUIRED_ENV[run_mode]:
            print(f"  {var}={os.environ.get(var)}")


def main():
    print("=" * 50)
    print("plus-worker starting")
    print("=" * 50)

    check_env()

    run_mode = os.environ.get('RUN_MODE')
    print(f"\nRUN_MODE: {run_mode}")

    # TEST MODE: Just sleep to prove the plumbing works
    print("\n[TEST MODE] Sleeping for 20 seconds...")
    for i in range(20):
        print(f"  {i+1}/20 seconds elapsed")
        time.sleep(1)

    print("\n[TEST MODE] Sleep complete. Exiting successfully.")
    print("=" * 50)
    sys.exit(0)


if __name__ == '__main__':
    main()
