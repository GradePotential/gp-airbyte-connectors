/*
 * Copyright (c) 2026 Grade Potential Tutoring. All rights reserved.
 */

package io.airbyte.integrations.source.mssqlct

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.discover.Field
import io.airbyte.cdk.util.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging

private val log = KotlinLogging.logger {}

data class MsSqlServerJdbcStreamStateValue(
    @JsonProperty("cursor") val cursor: JsonNode? = null,
    @JsonProperty("version") val version: Int = CURRENT_VERSION,
    @JsonProperty("state_type") val stateType: String = StateType.CURSOR_BASED.stateType,
    @JsonProperty("cursor_field") val cursorField: List<String> = listOf(),
    @JsonProperty("cursor_record_count") val cursorRecordCount: Int = 0,
    @JsonProperty("pk_name") val pkName: String? = null,
    @JsonProperty("pk_val") val pkValue: JsonNode? = null,
    @JsonProperty("incremental_state") val incrementalState: JsonNode? = null,
) {
    companion object {
        const val CURRENT_VERSION = 3
        const val LEGACY_VERSION = 2

        fun isLegacy(version: Int?): Boolean = version == null || version <= LEGACY_VERSION

        val snapshotCompleted: OpaqueStateValue
            get() = Jsons.valueToTree(MsSqlServerJdbcStreamStateValue(stateType = "primary_key"))

        fun cursorIncrementalCheckpoint(
            cursor: Field,
            cursorCheckpoint: JsonNode,
        ): OpaqueStateValue {
            return when (cursorCheckpoint.isNull) {
                true -> Jsons.nullNode()
                false ->
                    Jsons.valueToTree(
                        MsSqlServerJdbcStreamStateValue(
                            cursorField = listOf(cursor.id),
                            cursor = cursorCheckpoint,
                        )
                    )
            }
        }

        fun snapshotCheckpoint(
            primaryKey: List<Field>,
            primaryKeyCheckpoint: List<JsonNode>,
        ): OpaqueStateValue {
            val primaryKeyField = primaryKey.first()
            val pkNode = primaryKeyCheckpoint.first()
            return when (pkNode.isNull) {
                true -> Jsons.nullNode()
                false ->
                    Jsons.valueToTree(
                        MsSqlServerJdbcStreamStateValue(
                            pkName = primaryKeyField.id,
                            pkValue = pkNode,
                            stateType = StateType.PRIMARY_KEY.stateType,
                        )
                    )
            }
        }

        fun snapshotWithCursorCheckpoint(
            primaryKey: List<Field>,
            primaryKeyCheckpoint: List<JsonNode>,
            cursor: Field,
        ): OpaqueStateValue {
            val primaryKeyField = primaryKey.first()
            val pkNode = primaryKeyCheckpoint.first()
            return when (pkNode.isNull) {
                true -> Jsons.nullNode()
                false ->
                    Jsons.valueToTree(
                        MsSqlServerJdbcStreamStateValue(
                            pkName = primaryKeyField.id,
                            pkValue = pkNode,
                            stateType = StateType.PRIMARY_KEY.stateType,
                            incrementalState =
                                Jsons.valueToTree(
                                    MsSqlServerJdbcStreamStateValue(
                                        cursorField = listOf(cursor.id),
                                    )
                                ),
                        )
                    )
            }
        }
    }
}

enum class StateType(val stateType: String) {
    PRIMARY_KEY("primary_key"),
    CURSOR_BASED("cursor_based"),
}
