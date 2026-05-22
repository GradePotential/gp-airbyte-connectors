/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssqlct

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.StreamIdentifier
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.discover.Field
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.jdbc.JdbcFieldType
import io.airbyte.cdk.read.ConfiguredSyncMode
import io.airbyte.cdk.read.DefaultJdbcSharedState
import io.airbyte.cdk.read.DefaultJdbcStreamState
import io.airbyte.cdk.read.From
import io.airbyte.cdk.read.JdbcPartitionFactory
import io.airbyte.cdk.read.SelectColumnMaxValue
import io.airbyte.cdk.read.SelectQuerySpec
import io.airbyte.cdk.read.Stream
import io.airbyte.cdk.read.StreamFeedBootstrap
import io.airbyte.cdk.util.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Singleton

@Primary
@Singleton
class MsSqlServerJdbcPartitionFactory(
    override val sharedState: DefaultJdbcSharedState,
    val selectQueryGenerator: MsSqlSourceOperations,
    val config: MsSqlServerSourceConfiguration,
    val metadataQuerierFactory: MsSqlSourceMetadataQuerier.Factory,
) :
    JdbcPartitionFactory<
        DefaultJdbcSharedState,
        DefaultJdbcStreamState,
        MsSqlServerJdbcPartition,
    > {
    private val log = KotlinLogging.logger {}

    private val metadataQuerier: MsSqlSourceMetadataQuerier by lazy {
        metadataQuerierFactory.session(config) as MsSqlSourceMetadataQuerier
    }

    private val streamStates = ConcurrentHashMap<StreamIdentifier, DefaultJdbcStreamState>()

    override fun streamState(streamFeedBootstrap: StreamFeedBootstrap): DefaultJdbcStreamState =
        streamStates.getOrPut(streamFeedBootstrap.feed.id) {
            DefaultJdbcStreamState(sharedState, streamFeedBootstrap)
        }

    /** Detects if a stream corresponds to a SQL Server VIEW (vs a TABLE). */
    private fun isView(stream: Stream): Boolean {
        val tableName = metadataQuerier.findTableName(stream.id) ?: return false
        return tableName.type.equals("VIEW", ignoreCase = true)
    }

    /**
     * Returns the ordered column (from clustered index or PK) as a single-element list, or null if
     * no ordered column is available. Used for resumable partitioning.
     */
    private fun getOrderedColumnAsList(stream: Stream): List<Field>? {
        val orderedColumnName = metadataQuerier.getOrderedColumnForSync(stream.id) ?: return null
        val orderedColumn = stream.fields.find { it.id == orderedColumnName } ?: return null
        return listOf(orderedColumn)
    }

    private fun findPkUpperBound(stream: Stream): JsonNode {
        val orderedColumnName = metadataQuerier.getOrderedColumnForSync(stream.id)!!
        val orderedColumnForSync = stream.fields.find { it.id == orderedColumnName }!!

        val jdbcConnectionFactory = JdbcConnectionFactory(config)
        val from = From(stream.name, stream.namespace)
        val maxPkQuery = SelectQuerySpec(SelectColumnMaxValue(orderedColumnForSync), from)

        jdbcConnectionFactory.get().use { connection ->
            val stmt = connection.prepareStatement(selectQueryGenerator.generate(maxPkQuery).sql)
            val rs = stmt.executeQuery()

            if (rs.next()) {
                val jdbcFieldType = orderedColumnForSync.type as JdbcFieldType<*>
                return jdbcFieldType.get(rs, 1)
            } else {
                return Jsons.nullNode()
            }
        }
    }

    private fun coldStart(streamState: DefaultJdbcStreamState): MsSqlServerJdbcPartition {
        val stream: Stream = streamState.stream
        val isView = isView(stream)
        val orderedColumns = getOrderedColumnAsList(stream)

        if (stream.configuredSyncMode == ConfiguredSyncMode.FULL_REFRESH) {
            if (isView || orderedColumns == null) {
                return MsSqlServerJdbcNonResumableSnapshotPartition(
                    selectQueryGenerator,
                    streamState,
                )
            }
            val upperBound = findPkUpperBound(stream)
            return MsSqlServerJdbcRfrSnapshotPartition(
                selectQueryGenerator,
                streamState,
                orderedColumns,
                lowerBound = null,
                upperBound = listOf(upperBound),
            )
        }

        val cursorChosenFromCatalog: Field =
            stream.configuredCursor as? Field ?: throw ConfigErrorException("no cursor")

        val cursorCutoffTime = getCursorCutoffTime(cursorChosenFromCatalog)

        if (isView || orderedColumns == null) {
            return MsSqlServerJdbcNonResumableSnapshotWithCursorPartition(
                selectQueryGenerator,
                streamState,
                cursorChosenFromCatalog,
                cursorCutoffTime = cursorCutoffTime,
            )
        }

        return MsSqlServerJdbcSnapshotWithCursorPartition(
            selectQueryGenerator,
            streamState,
            orderedColumns,
            lowerBound = null,
            cursorChosenFromCatalog,
            cursorUpperBound = null,
            cursorCutoffTime = cursorCutoffTime,
        )
    }

    /**
     * Creates or resumes a Change Tracking partition.
     *
     * State machine:
     * 1. `opaqueState == null` → cold start: capture current CT version, begin PK snapshot.
     * 2. `ctState.pkName != null` → snapshot in progress: resume from last PK checkpoint.
     * 3. `ctState.pkName == null` + has PK → snapshot complete: run CHANGETABLE incremental read.
     * 4. `ctState.pkName == null` + no PK + CT version unchanged → snapshot just finished this
     *    sync cycle: return null (stream done).
     * 5. `ctState.pkName == null` + no PK + CT version advanced → re-run full non-resumable
     *    snapshot (behaves like FULL_REFRESH on each sync).
     *
     * On each transition the stored CT version is validated against
     * [MsSqlSourceMetadataQuerier.getMinValidCtVersion]. If the version has expired the behavior
     * depends on [InvalidCtVersionBehavior] (fail or reset to full re-sync).
     */
    private fun createCtPartition(
        stream: Stream,
        streamState: DefaultJdbcStreamState,
        opaqueState: OpaqueStateValue?,
    ): MsSqlServerJdbcPartition? {
        val ctConfig =
            config.incrementalReplicationConfiguration as ChangeTrackingIncrementalConfiguration

        // ----- Cold start -----
        if (opaqueState == null || opaqueState.isNull) {
            return ctColdStart(stream, streamState)
        }

        val ctState =
            try {
                Jsons.treeToValue(opaqueState, MsSqlServerJdbcCtStreamState::class.java)
            } catch (e: Exception) {
                log.warn(e) {
                    "Failed to parse CT state for ${stream.name}, resetting to cold start"
                }
                return ctColdStart(stream, streamState)
            }

        // Validate stored CT version is still within retention window
        val tableObjectId = metadataQuerier.getTableObjectId(stream.namespace, stream.name)
        if (tableObjectId != null) {
            val minValid = metadataQuerier.getMinValidCtVersion(tableObjectId)
            if (ctState.ctVersion < minValid) {
                log.warn {
                    "CT version ${ctState.ctVersion} for ${stream.name} has expired " +
                        "(min valid: $minValid). Behavior: ${ctConfig.invalidCtVersionBehavior}"
                }
                return when (ctConfig.invalidCtVersionBehavior) {
                    InvalidCtVersionBehavior.FAIL_SYNC ->
                        throw ConfigErrorException(
                            "Change Tracking version ${ctState.ctVersion} for stream " +
                                "'${stream.name}' has expired. The minimum valid version is " +
                                "$minValid. Increase CHANGE_RETENTION_DAYS on the database or " +
                                "reduce the sync interval. Alternatively, change " +
                                "'Invalid CT Version Behavior' to 'Re-sync data' to automatically " +
                                "trigger a full re-sync."
                        )
                    InvalidCtVersionBehavior.RESET_SYNC -> {
                        log.warn { "Resetting CT sync for ${stream.name} to full re-sync." }
                        ctColdStart(stream, streamState)
                    }
                }
            }
        }

        // ----- Resume snapshot -----
        if (ctState.pkName != null) {
            val orderedColumns = getOrderedColumnAsList(stream)
            if (orderedColumns == null) {
                log.warn {
                    "Stream ${stream.name} has no PK/clustered index. " +
                        "Using non-resumable snapshot for CT mode."
                }
                // CT version was captured at snapshot start; preserve it in the new state.
                val currentCtVersion = metadataQuerier.getCurrentCtVersion()
                return MsSqlServerJdbcCtNonResumableSnapshotPartition(
                    selectQueryGenerator, streamState, currentCtVersion
                )
            }
            val primaryKey = orderedColumns.first()
            val lowerBound =
                ctState.pkVal?.let { listOf(stateValueToJsonNode(primaryKey, it)) }
            return MsSqlServerJdbcCtSnapshotPartition(
                selectQueryGenerator,
                streamState,
                primaryKey,
                lowerBound = lowerBound,
                capturedCtVersion = ctState.ctVersion,
            )
        }

        // ----- CT incremental -----
        val primaryKeyFields = getPrimaryKeyFields(stream)
        if (primaryKeyFields.isEmpty()) {
            // No PK: CT incremental (CHANGETABLE) is not possible.
            //
            // Use the current CT version to determine whether the snapshot for this sync cycle
            // has already run:
            //   - If currentVersion == ctState.ctVersion, the snapshot just completed this cycle
            //     (state was saved with the version captured at snapshot start, and the CT version
            //     hasn't advanced in the milliseconds since). Return null → stream is done.
            //   - If currentVersion != ctState.ctVersion, either this is a new sync or the DB has
            //     advanced. Re-run a full non-resumable snapshot to pick up any changes.
            val currentVersion = metadataQuerier.getCurrentCtVersion()
            if (currentVersion == ctState.ctVersion) {
                log.info {
                    "Stream ${stream.name}: no-PK table, CT version unchanged at $currentVersion. " +
                        "Snapshot complete for this sync cycle."
                }
                return null
            }
            log.warn {
                "Stream ${stream.name}: no-PK table, CT version advanced " +
                    "${ctState.ctVersion} → $currentVersion. Re-running full snapshot."
            }
            return MsSqlServerJdbcCtNonResumableSnapshotPartition(
                selectQueryGenerator, streamState, currentVersion
            )
        }

        // Tables with a PK but without per-table CT enabled cannot use CHANGETABLE.
        // Fall back to the same version-based full-refresh logic as no-PK tables.
        if (!metadataQuerier.isTableCtEnabled(stream.namespace, stream.name)) {
            val currentVersion = metadataQuerier.getCurrentCtVersion()
            if (currentVersion == ctState.ctVersion) {
                log.info {
                    "Stream ${stream.name}: CT not enabled on table, version unchanged at " +
                        "$currentVersion. Snapshot complete for this sync cycle."
                }
                return null
            }
            log.warn {
                "Stream ${stream.name}: CT not enabled on table, CT version advanced " +
                    "${ctState.ctVersion} → $currentVersion. Re-running full snapshot."
            }
            return MsSqlServerJdbcCtNonResumableSnapshotPartition(
                selectQueryGenerator, streamState, currentVersion
            )
        }

        val nextVersion = metadataQuerier.getCurrentCtVersion()
        if (nextVersion == ctState.ctVersion) {
            log.info { "No new CT changes for ${stream.name} (version unchanged: $nextVersion)" }
            return null
        }

        return MsSqlServerJdbcCtIncrementalPartition(
            selectQueryGenerator,
            streamState,
            primaryKeys = primaryKeyFields,
            sinceVersion = ctState.ctVersion,
            nextVersion = nextVersion,
        )
    }

    /** Initial CT sync: capture current version, start PK-ordered snapshot. */
    private fun ctColdStart(
        stream: Stream,
        streamState: DefaultJdbcStreamState,
    ): MsSqlServerJdbcPartition? {
        val currentCtVersion = metadataQuerier.getCurrentCtVersion()
        log.info { "CT cold start for ${stream.name}: captured version $currentCtVersion" }

        val orderedColumns = getOrderedColumnAsList(stream)
        if (orderedColumns == null || isView(stream)) {
            log.warn {
                "Stream ${stream.name} has no PK/clustered index. " +
                    "Using non-resumable snapshot (CT version $currentCtVersion captured)."
            }
            // Use the CT-aware non-resumable partition so the captured CT version is preserved
            // in state. This lets subsequent calls detect "snapshot just finished this cycle"
            // vs "new sync, re-run snapshot" by comparing stored vs current CT version.
            return MsSqlServerJdbcCtNonResumableSnapshotPartition(
                selectQueryGenerator, streamState, currentCtVersion
            )
        }

        return MsSqlServerJdbcCtSnapshotPartition(
            selectQueryGenerator,
            streamState,
            primaryKey = orderedColumns.first(),
            lowerBound = null,
            capturedCtVersion = currentCtVersion,
        )
    }

    /** Returns the stream's primary key fields in order. */
    private fun getPrimaryKeyFields(stream: Stream): List<Field> {
        val pkColumns = metadataQuerier.primaryKey(stream.id)
        return pkColumns.mapNotNull { pkCol ->
            val colName = pkCol.firstOrNull() ?: return@mapNotNull null
            stream.fields.find { it.id == colName }
        }
    }

    override fun create(streamFeedBootstrap: StreamFeedBootstrap): MsSqlServerJdbcPartition? {
        val stream: Stream = streamFeedBootstrap.feed
        val streamState: DefaultJdbcStreamState = streamState(streamFeedBootstrap)
        val opaqueStateValue: OpaqueStateValue? = streamFeedBootstrap.currentState

        if (opaqueStateValue?.isNull == true) {
            return null
        }

        // ----- Change Tracking mode -----
        if (config.incrementalReplicationConfiguration is ChangeTrackingIncrementalConfiguration) {
            return createCtPartition(stream, streamState, opaqueStateValue)
        }

        // ----- Cursor-based mode -----
        if (opaqueStateValue == null) {
            return coldStart(streamState)
        }

        val isView = isView(stream)
        val orderedColumns = getOrderedColumnAsList(stream)

        if (
            stream.configuredSyncMode == ConfiguredSyncMode.FULL_REFRESH &&
                (isView || orderedColumns == null)
        ) {
            return handleFullRefreshWithoutPk(streamState)
        }

        val sv: MsSqlServerJdbcStreamStateValue =
            MsSqlServerStateMigration.parseStateValue(opaqueStateValue)

        if (stream.configuredSyncMode == ConfiguredSyncMode.FULL_REFRESH) {
            val upperBound = findPkUpperBound(stream)
            val pkLowerBound: JsonNode =
                extractPkLowerBound(sv.pkValue, orderedColumns!!.first())

            if (!pkLowerBound.isNull && areValuesEqual(pkLowerBound, upperBound)) {
                return null
            }

            return MsSqlServerJdbcRfrSnapshotPartition(
                selectQueryGenerator,
                streamState,
                orderedColumns,
                lowerBound = if (pkLowerBound.isNull) null else listOf(pkLowerBound),
                upperBound = listOf(upperBound),
            )
        }

        if (sv.pkName != null) {
            val cursorChosenFromCatalog: Field =
                stream.configuredCursor as? Field ?: throw ConfigErrorException("no cursor")

            if (isView(stream) || orderedColumns == null) {
                return MsSqlServerJdbcNonResumableSnapshotWithCursorPartition(
                    selectQueryGenerator,
                    streamState,
                    cursorChosenFromCatalog,
                    cursorCutoffTime = getCursorCutoffTime(cursorChosenFromCatalog),
                )
            }

            val pkLowerBound: JsonNode = extractPkLowerBound(sv.pkValue, orderedColumns.first())

            return MsSqlServerJdbcSnapshotWithCursorPartition(
                selectQueryGenerator,
                streamState,
                orderedColumns,
                lowerBound = listOf(pkLowerBound),
                cursorChosenFromCatalog,
                cursorUpperBound = null,
                cursorCutoffTime = getCursorCutoffTime(cursorChosenFromCatalog),
            )
        }

        if (sv.cursorField.isEmpty()) {
            log.info {
                "State has empty cursor_field for stream ${stream.name}, sync already complete"
            }
            return null
        }

        val cursor: Field? = stream.fields.find { it.id == sv.cursorField.first() }
        if (cursor == null) {
            log.warn {
                "Cursor field '${sv.cursorField.first()}' not found in stream ${stream.name}, resetting stream"
            }
            streamState.reset()
            return coldStart(streamState)
        }

        val cursorCheckpoint: JsonNode =
            if (sv.cursor == null || sv.cursor.isNull) {
                Jsons.nullNode()
            } else {
                stateValueToJsonNode(cursor, sv.cursor.asText())
            }

        val upperBound = streamState.cursorUpperBound
        if (upperBound != null) {
            if (areValuesEqual(cursorCheckpoint, upperBound)) {
                return null
            }
        }
        return MsSqlServerJdbcCursorIncrementalPartition(
            selectQueryGenerator,
            streamState,
            cursor,
            cursorLowerBound = cursorCheckpoint,
            isLowerBoundIncluded = false,
            cursorUpperBound = streamState.cursorUpperBound,
            cursorCutoffTime = getCursorCutoffTime(cursor),
        )
    }

    private fun handleFullRefreshWithoutPk(
        streamState: DefaultJdbcStreamState
    ): MsSqlServerJdbcPartition? {
        if (
            streamState.streamFeedBootstrap.currentState ==
                MsSqlServerJdbcStreamStateValue.snapshotCompleted
        ) {
            return null
        }
        return MsSqlServerJdbcNonResumableSnapshotPartition(
            selectQueryGenerator,
            streamState,
        )
    }

    private fun extractPkLowerBound(pkValue: JsonNode?, orderedColumnForSync: Field): JsonNode {
        return when {
            pkValue == null || pkValue.isNull -> Jsons.nullNode()
            pkValue.isTextual -> stateValueToJsonNode(orderedColumnForSync, pkValue.asText())
            else -> pkValue
        }
    }

    private fun getCursorCutoffTime(cursorField: Field): JsonNode? {
        val incrementalConfig = config.incrementalReplicationConfiguration
        return if (
            incrementalConfig is UserDefinedCursorIncrementalConfiguration &&
                incrementalConfig.excludeTodaysData &&
                MsSqlServerCursorCutoffTimeProvider.isTemporalType(cursorField)
        ) {
            val cutoffTime = MsSqlServerCursorCutoffTimeProvider.getCutoffTime(cursorField)
            log.info { "Using cursor cutoff time: $cutoffTime for field '${cursorField.id}'" }
            cutoffTime
        } else {
            null
        }
    }

    private fun areValuesEqual(a: JsonNode, b: JsonNode): Boolean {
        if (a.isNumber && b.isNumber) {
            return try {
                a.decimalValue().compareTo(b.decimalValue()) == 0
            } catch (e: Exception) {
                log.warn(e) {
                    "Failed to compare numeric values, falling back to string comparison"
                }
                a.toString() == b.toString()
            }
        }
        return a == b
    }

    override fun split(
        unsplitPartition: MsSqlServerJdbcPartition,
        opaqueStateValues: List<OpaqueStateValue>
    ): List<MsSqlServerJdbcPartition> {
        return when (unsplitPartition) {
            is MsSqlServerJdbcRfrSnapshotPartition -> unsplitPartition.split(opaqueStateValues)
            is MsSqlServerJdbcSnapshotWithCursorPartition ->
                unsplitPartition.split(opaqueStateValues)
            is MsSqlServerJdbcSplittableSnapshotWithCursorPartition -> listOf(unsplitPartition)
            is MsSqlServerJdbcCursorIncrementalPartition -> listOf(unsplitPartition)
            is MsSqlServerJdbcNonResumableSnapshotPartition -> listOf(unsplitPartition)
            is MsSqlServerJdbcNonResumableSnapshotWithCursorPartition -> listOf(unsplitPartition)
            // CT partitions are not split
            is MsSqlServerJdbcCtSnapshotPartition -> listOf(unsplitPartition)
            is MsSqlServerJdbcCtIncrementalPartition -> listOf(unsplitPartition)
            is MsSqlServerJdbcCtNonResumableSnapshotPartition -> listOf(unsplitPartition)
        }
    }

    companion object {
        const val DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
        val outputDateFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern(DATETIME_PATTERN)

        val TIMESTAMP_WITHOUT_FRACT_SECOND_PATTERN = "yyyy-MM-dd'T'HH:mm:ss"
        val inputDateFormatter: DateTimeFormatter =
            DateTimeFormatterBuilder()
                .appendPattern(TIMESTAMP_WITHOUT_FRACT_SECOND_PATTERN)
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 1, 6, true)
                .optionalEnd()
                .toFormatter()

        val timestampWithoutTimezoneParser: DateTimeFormatter =
            DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .toFormatter()

        val timestampWithTimezoneParser: DateTimeFormatter =
            DateTimeFormatterBuilder()
                .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
                .optionalStart()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                .optionalEnd()
                .appendOffset("+HH:MM", "Z")
                .toFormatter()
    }
}
