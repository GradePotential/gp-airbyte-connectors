/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mssqlct

import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.discover.Field
import io.airbyte.cdk.jdbc.BigIntegerFieldType
import io.airbyte.cdk.jdbc.StringFieldType
import io.airbyte.cdk.read.DefaultJdbcStreamState
import io.airbyte.cdk.read.SelectQuery
import io.airbyte.cdk.read.SelectQueryGenerator
import io.airbyte.cdk.util.Jsons

/**
 * Partition for the Change Tracking incremental phase.
 *
 * Executes a `CHANGETABLE(CHANGES ...)` query to retrieve all INSERTs, UPDATEs, and DELETEs since
 * [sinceVersion]. Results include a computed `_ab_cdc_deleted_at` column that is non-null for
 * deleted rows, enabling soft-delete detection in downstream silver views.
 *
 * The [nextVersion] is captured before the read begins (via
 * [MsSqlSourceMetadataQuerier.getCurrentCtVersion]) so that concurrent changes during the read are
 * picked up in the next sync cycle.
 *
 * This partition is non-splittable: all changes are read in a single pass. If interrupted,
 * [incompleteState] returns [sinceVersion] so the changes are re-read safely on the next run
 * (CHANGETABLE is idempotent — re-reading the same version range returns the same rows).
 */
class MsSqlServerJdbcCtIncrementalPartition(
    selectQueryGenerator: SelectQueryGenerator,
    streamState: DefaultJdbcStreamState,
    val primaryKeys: List<Field>,
    /** CT version to start from (exclusive — changes > sinceVersion are returned). */
    val sinceVersion: Long,
    /**
     * CT version captured at partition creation time. Saved as the new state after a successful
     * read.
     */
    val nextVersion: Long,
) : MsSqlServerJdbcPartition(selectQueryGenerator, streamState) {

    companion object {
        /** Synthetic field name for soft-delete detection. */
        const val DELETED_AT_FIELD = "_ab_cdc_deleted_at"
    }

    /**
     * Builds the CHANGETABLE SQL with:
     * - COALESCE(t.[pk], CT.[pk]) for each PK column (ensures PK is present for deletes)
     * - t.[non_pk_col] for every non-PK column (NULL for deletes — fine for downstream)
     * - A CASE expression for `_ab_cdc_deleted_at`
     */
    override val nonResumableQuery: SelectQuery
        get() {
            val pkNames = primaryKeys.map { it.id }.toSet()

            val ns = stream.namespace
            val tableSql =
                if (ns != null) "${ns.quoted()}.[${stream.name.replace("]", "]]")}]"
                else "[${stream.name.replace("]", "]]")}]"

            // Soft-delete marker: ISO 8601 UTC timestamp for D rows, NULL otherwise
            val deletedAtCol =
                "CASE WHEN CT.SYS_CHANGE_OPERATION = 'D' " +
                    "THEN CONVERT(NVARCHAR(50), GETUTCDATE(), 127) " +
                    "ELSE NULL END AS [_ab_cdc_deleted_at]"

            // JOIN predicate on PK
            val joinOn =
                primaryKeys.joinToString(" AND ") { pk ->
                    val q = pk.id.quoted()
                    "t.$q = CT.$q"
                }

            // Emit columns in stream.fields order so CDK positional reads match correctly.
            // PK fields use COALESCE(t.pk, CT.pk) so they are non-null even for deletes.
            val allCols =
                stream.fields.joinToString(", ") { f ->
                    val q = f.id.quoted()
                    if (f.id in pkNames) "COALESCE(t.$q, CT.$q) AS $q" else "t.$q"
                }

            val selectCols = "$allCols, $deletedAtCol"

            val sql =
                "SELECT $selectCols " +
                    "FROM CHANGETABLE(CHANGES $tableSql, ?) AS CT " +
                    "LEFT JOIN $tableSql t ON $joinOn " +
                    "ORDER BY CT.SYS_CHANGE_VERSION ASC"

            // Columns: all stream fields + the synthetic deleted-at field
            val columns = stream.fields + Field(DELETED_AT_FIELD, StringFieldType)

            // Binding for sinceVersion parameter
            val sinceVersionNode = Jsons.valueToTree<JsonNode>(sinceVersion.toBigInteger())
            val bindings = listOf(SelectQuery.Binding(sinceVersionNode, BigIntegerFieldType))

            return SelectQuery(sql, columns, bindings)
        }

    /** After a complete read, advance the CT version to what was current at read start. */
    override val completeState: OpaqueStateValue
        get() = MsSqlServerJdbcCtStreamState.incrementalCheckpoint(nextVersion)
    // CT incremental is non-splittable (JdbcPartition, not JdbcSplittablePartition).
    // If interrupted, the stored state retains sinceVersion so CHANGETABLE is re-read
    // idempotently on the next sync.
}

/** Quotes a SQL Server identifier with square brackets, escaping embedded ']'. */
private fun String.quoted(): String = "[${this.replace("]", "]]")}]"
