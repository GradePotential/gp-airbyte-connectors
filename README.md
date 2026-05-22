# GP Airbyte Connectors

Custom Airbyte source connectors for the data pipeline. Each connector is a standalone Gradle subproject that builds to a Docker image and deploys to a self-hosted Airbyte instance running in Kubernetes on a GCP VM.

## Connectors

| Connector | Language | CDK | Description |
|-----------|----------|-----|-------------|
| `source-mssql-ct` | Kotlin | bulk-cdk `0.2.4` | SQL Server with Change Tracking incremental sync. Used where CDC is unavailable (Web Edition). Emits `_ab_cdc_deleted_at` for soft-delete detection. |
| `source-mongodb-v2` | Java | java-cdk `0.48.9` | MongoDB patched for a custom TLS CA certificate and JVM memory limits. |

---

## One-time setup

**Prerequisites:** Java 21, Docker Desktop, SSH key with access to the Airbyte VM.

Copy the deploy config template and fill in your values. This is a one-time step — once `deploy.env` exists the scripts handle everything.

```bash
cp deploy.env.example deploy.env
# edit deploy.env — gitignored, never commit it
```

`deploy.env.example` documents every variable: VM address, SSH key path, GCP project, Airbyte k8s details, and connector DB record IDs. The comments tell you exactly where to find each value.

---

## Making and deploying a change

This is the full end-to-end flow for a change to an existing connector. Using MongoDB as the example — substitute `source-mssql-ct` and `deploy-mssql-ct.sh` for the other connector.

### Step 1 — Test your change locally

Run unit tests:

```bash
./gradlew :sources:source-mongodb-v2:test
```

Optionally test against a live database connection. Create a `config.json` with your connection details (do not commit it) and use the Gradle `run` task, which invokes the connector directly without Docker:

```bash
# Verify the connection is valid
./gradlew :sources:source-mongodb-v2:run --args="check --config /path/to/config.json"

# Inspect the discovered schema
./gradlew :sources:source-mongodb-v2:run --args="discover --config /path/to/config.json"

# Read records (needs a catalog.json — copy from the discover output)
./gradlew :sources:source-mongodb-v2:run --args="read --config /path/to/config.json --catalog /path/to/catalog.json"

# Print the connector spec (no config needed)
./gradlew :sources:source-mongodb-v2:run --args="spec"
```

### Step 2 — Deploy

Pick the next version number (`1.0.6`, `1.1.0`, etc.) and run the deploy script. It handles everything in one shot: builds the JAR, builds the Docker image, transfers it to the VM, and updates the version record in the Airbyte DB.

```bash
./scripts/deploy-mongodb.sh 1.0.6
```

Use patch bumps (`1.0.x`) for bug fixes, minor bumps (`1.x.0`) for new behaviour or catalog changes.

### Step 3 — Verify

Check the new version is registered in Airbyte. SSH to the VM and run:

```bash
docker exec airbyte-abctl-control-plane \
  kubectl exec -n airbyte-abctl airbyte-db-0 -- \
  psql -U airbyte -d db-airbyte -c \
  "SELECT docker_image_tag FROM actor_definition_version WHERE id='<MONGODB_ACTOR_DEF_VERSION_ID>';"
```

Then trigger a manual sync in the Airbyte UI and check the job logs to confirm the new image runs cleanly.

### Step 4 — Schema refresh (only if you added or renamed fields)

If your change added a new column to the connector's catalog, go to the connection in the Airbyte UI:

> Connection → Schema tab → Refresh source schema → Save

This adds the new column to the BigQuery destination tables via `ALTER TABLE`. It does not reset sync state or trigger a re-snapshot.

---

## Project structure

```
gp-airbyte-connectors/
├── buildSrc/                   # Gradle plugins (adapted from Airbyte OSS)
│   └── src/main/groovy/
│       ├── airbyte-bulk-connector.gradle   # for bulk-CDK connectors (source-mssql-ct)
│       └── airbyte-java-connector.gradle   # for java-CDK connectors (source-mongodb-v2)
│
├── sources/
│   ├── source-mssql-ct/        # SQL Server + Change Tracking connector
│   └── source-mongodb-v2/      # MongoDB + ScaleGrid TLS connector
│
├── scripts/
│   ├── deploy-mssql-ct.sh
│   └── deploy-mongodb.sh
│
├── deploy.env.example          # Template — copy to deploy.env and fill in values
└── deploy.env                  # Your local config (gitignored)
```

### Notable files inside each connector

| File | Purpose |
|------|---------|
| `build.gradle` | CDK plugin, version, and connector-specific dependencies |
| `gradle.properties` | Sets `cdkVersion` — bump this when upgrading the CDK |
| `Dockerfile.amd64` | Production image for `linux/amd64`. Copies the Gradle distribution tar from `build/distributions/` |
| `metadata.yaml` | Connector metadata read by Airbyte (name, sync modes, `definitionId`) |
| `scalegrid-ca.crt` | *(source-mongodb-v2 only)* ScaleGrid public CA cert baked into the image for TLS |

---

## Registering a connector for the first time

The deploy scripts assume the connector already exists in Airbyte's DB. For a brand new connector, register it once via the Airbyte API (port-forward 8001 from the VM, or run from the VM directly):

```bash
curl -s -X POST http://localhost:8001/api/v1/source_definitions/create_custom \
  -H 'Content-Type: application/json' \
  -d '{
    "workspaceId": "<your-workspace-id>",
    "sourceDefinition": {
      "name": "My Connector Name",
      "dockerRepository": "<registry>/<repo>/<connector-name>",
      "dockerImageTag": "1.0.0",
      "documentationUrl": "https://docs.airbyte.com"
    }
  }'
```

Then get the `actor_definition_version` ID from the DB:

```sql
SELECT id, docker_image_tag
FROM actor_definition_version
WHERE docker_repository LIKE '%<connector-name>%';
```

Add that ID to `deploy.env`. All future version bumps go through the deploy script.

---

## Adding a new connector

### 1. Choose the right CDK and language

| Use case | CDK | Gradle plugin |
|----------|-----|---------------|
| New JDBC/SQL source | **bulk-cdk** | `airbyte-bulk-connector` |
| New non-JDBC source | **java-cdk** | `airbyte-java-connector` |

**Any JVM language works** — the CDK contract is interfaces and abstract classes. Kotlin is the natural fit for bulk-cdk (the CDK itself is Kotlin and uses Micronaut with KSP for DI). Java works but requires swapping `ksp` for `annotationProcessor` in `build.gradle`. IntelliJ handles either.

### 2. Key types to implement (bulk-cdk / JDBC sources)

All in the `io.airbyte.cdk` packages published under `io.airbyte.bulk-cdk`:

| Type | What it controls |
|------|-----------------|
| `JdbcSourceConfiguration` | Runtime config — host, port, credentials, options |
| `SourceConfigurationSpecification` | JSON schema for the Airbyte UI connection form |
| `JdbcMetadataQuerier.FieldTypeMapper` | Maps JDBC column types → Airbyte field types during discovery |
| `JdbcAirbyteStreamFactory` | Turns discovered tables into `AirbyteStream` definitions |
| `SelectQueryGenerator` | Generates the SQL `SELECT` queries used during reads |
| `MetadataQuerier` | Wraps `JdbcMetadataQuerier` to customise table/schema discovery |
| `JdbcPartitionFactory<S, T, P>` | **Core incremental logic** — decides snapshot vs. incremental and creates read partitions |

Entry point: just call `AirbyteSourceRunner.run(*args)` from `main()`. Annotate implementations with `@Singleton` and `@Primary` so Micronaut discovers them.

For java-cdk sources, implement `Source`: `checkConnection()`, `discover()`, `read()`. See `source-mongodb-v2` as a working reference.

### 3. Scaffold

```
sources/source-my-connector/
├── src/main/kotlin/...     # (or java/)
├── build.gradle            # copy from an existing connector, update plugin and deps
├── gradle.properties       # set cdkVersion
├── Dockerfile.amd64        # copy from an existing connector, update connector name
└── metadata.yaml           # set name, dockerRepository, and a new definitionId UUID
```

Add to `settings.gradle`:
```groovy
include ':sources:source-my-connector'
```

Add a deploy script to `scripts/` following the existing pattern, and add the connector's `ACTOR_DEF_VERSION_ID` variable to `deploy.env.example`.

### 4. CDK version reference

- **bulk-cdk versions:** `https://airbyte.mycloudrepo.io/public/repositories/airbyte-public-jars/io/airbyte/bulk-cdk/`
- **java-cdk versions:** https://github.com/airbytehq/airbyte/releases

When upgrading the bulk-cdk, also update `micronautVersion` in `buildSrc/src/main/groovy/airbyte-bulk-connector.gradle` if the Micronaut BOM version changed.
