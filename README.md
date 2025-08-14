# RHDL Operator

A Kubernetes operator for managing RHDL (Red Hat Downloader) download jobs. This
operator automates the scheduling and execution of content downloads from the
Red Hat Downloader service.

## Overview

The RHDL Operator provides a Kubernetes-native way to schedule and manage
downloads from RHDL topics. It creates and manages CronJobs that run the RHDL
CLI tool to download content on a scheduled basis, with support for persistent
storage and configurable authentication.

## Features

- **Automated Scheduling**: Schedule downloads using standard cron expressions
- **Topic-based Downloads**: Download specific RHDL topics with configurable tags
- **Persistent Storage**: Mount persistent volumes for downloaded content
- **Secure Authentication**: Use Kubernetes secrets for RHDL credentials
- **Customizable Execution**: Configure container images, pull policies, and extra arguments
- **Event Monitoring**: Track download job status through Kubernetes events

## Development Quick Start

### Prerequisites

- Kubernetes cluster (v1.24+)
- kubectl configured to access your cluster
- RHDL access credentials (access key and secret key)

### Installation

1. Install the Custom Resource Definitions (CRDs):
```bash
make install
```

2. Run the operator:
```bash
make run
```

### Basic Usage

1. Create a secret with your RHDL credentials:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: rhdl-credentials
  namespace: default
type: Opaque
data:
  RHDL_ACCESS_KEY: <base64-encoded-access-key>
  RHDL_SECRET_KEY: <base64-encoded-secret-key>
  # Optional: Custom API URL (defaults to https://api.rhdl.distributed-ci.io)
  RHDL_API_URL: <base64-encoded-api-url>
```

2. Create a PersistentVolumeClaim for storing downloaded content:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: rhdl-storage
  namespace: default
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
```

3. Create a Downloader resource:
```yaml
apiVersion: rhdl.distributed-ci.io/v1alpha1
kind: Downloader
metadata:
  name: rhel-downloader
  namespace: default
spec:
  topic: RHEL-9.2
  tag: milestone
  schedule: "@daily"
  persistentVolumeClaim: rhdl-storage
  credentials: rhdl-credentials
```

## Configuration

### Downloader Spec

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `topic` | string | RHDL topic name to download | Required |
| `tag` | string | Topic tag to download | `milestone` |
| `schedule` | string | Cron schedule expression | `@daily` |
| `persistentVolumeClaim` | string | PVC name for storage | Required |
| `credentials` | string | Secret name with RHDL credentials | `credentials` |
| `containerImage` | string | Container image for downloader | `quay.io/rhdl/cli:latest` |
| `pullPolicy` | string | Image pull policy | `Always` |
| `extraArgs` | []string | Additional CLI arguments | `[]` |

### Credentials Secret

The credentials secret must contain:
- `RHDL_ACCESS_KEY`: Your RHDL access key
- `RHDL_SECRET_KEY`: Your RHDL secret key
- `RHDL_API_URL` (optional): Custom API endpoint

### Schedule Format

The `schedule` field supports standard cron expressions and aliases:
- `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@hourly`
- Standard cron format e.g. `0 2 * * *` (daily at 2 AM)

## Examples

### Download Multiple Topics

```yaml
---
apiVersion: rhdl.distributed-ci.io/v1alpha1
kind: Downloader
metadata:
  name: rhel-9.2
spec:
  topic: RHEL-9.2
  schedule: "0 4 * * 1"  # Weekly on Monday at 4 AM
  persistentVolumeClaim: rhdl-storage
  credentials: rhdl-credentials
---
apiVersion: rhdl.distributed-ci.io/v1alpha1
kind: Downloader
metadata:
  name: rhel-9.4-nightly
spec:
  topic: RHEL-9.4
  tag: nightly
  schedule: "0 3 * * *"  # Daily at 3 AM
  persistentVolumeClaim: rhdl-storage
  credentials: rhdl-credentials
  extraArgs:
    - "--exclude='*'"
    - "--include='.composeinfo'"
```

## Development

### Building from Source

```bash
# Build the manager binary
make build

# Run locally (requires KUBECONFIG)
make install run

# Build Docker image
make docker-build IMG=my-registry/rhdl-operator:latest

# Push Docker image
make docker-push IMG=my-registry/rhdl-operator:latest
```

### Testing

```bash
# Run unit tests
make test

# Run end-to-end tests (requires Kind)
make test-e2e

# Run linter
make lint
```

### Generating Manifests

```bash
# Generate CRDs and RBAC manifests
make manifests

# Generate installation YAML
make build-installer
```

## Architecture

The RHDL Operator manages the following resources:

1. **Downloader CRD**: Defines the desired state for download jobs
2. **CronJob**: Executes scheduled downloads using the RHDL CLI
3. **Secret**: Stores RHDL authentication credentials
4. **PersistentVolumeClaim**: Provides storage for downloaded content

The operator watches for changes to Downloader resources and automatically
creates, updates, or deletes corresponding CronJobs to maintain the desired
state.

## Monitoring

The operator provides several ways to monitor download jobs:

### Kubernetes Events
```bash
kubectl get events --field-selector involvedObject.kind=Downloader
```

### CronJob Status
```bash
kubectl get cronjobs -l app.kubernetes.io/managed-by=rhdl-operator
```

### Job Logs
```bash
kubectl logs -l job-name=<cronjob-name>-<timestamp>
```

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes and add tests
4. Run the test suite: `make test lint`
5. Commit your changes: `git commit -am 'Add new feature'`
6. Push to the branch: `git push origin feature/my-feature`
7. Submit a pull request

## License

This project is licensed under the Apache License 2.0. See the
[LICENSE](LICENSE) file for details.

## Support

- Documentation: [https://rhdl.distributed-ci.io](https://rhdl.distributed-ci.io)
- Issues: [GitLab Issues](https://gitlab.cee.redhat.com/rhdl/operator/-/issues)
- RHDL Service: [https://rhdl.distributed-ci.io](https://rhdl.distributed-ci.io)
