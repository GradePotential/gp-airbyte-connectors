/*
 * Copyright (c) 2026 Grade Potential Tutoring. All rights reserved.
 */

package io.airbyte.integrations.source.mssqlct

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import io.airbyte.cdk.command.OpaqueStateValue
import io.airbyte.cdk.util.Jsons
import io.github.oshai.kotlinlogging.KotlinLogging

private val log = KotlinLogging.logger {}

data class LegacyOrderedColumnLoadStatus(
    @JsonProperty("version") val version: Long? = null,
    @JsonProperty("state_type") val stateType: String? = null,
    @JsonProperty("ordered_col") val orderedCol: String? = null,
    @JsonProperty("ordered_col_val") val orderedColVal: String? = null,
    @JsonProperty("incremental_state") val incrementalState: JsonNode? = null,
)

data class LegacyCursorBasedStatus(
    @JsonProperty("version") val version: Long? = null,
    @JsonProperty("state_type") val stateType: String? = null,
    @JsonProperty("stream_name") val streamName: String? = null,
    @JsonProperty("stream_namespace") val streamNamespace: String? = null,
    @JsonProperty("cursor_field") val cursorField: List<String>? = null,
    @JsonProperty("cursor") val cursor: String? = null,
    @JsonProperty("cursor_record_count") val cursorRecordCount: Long? = null,
)

object MsSqlServerStateMigration {

    fun parseStateValue(opaqueStateValue: OpaqueStateValue): MsSqlServerJdbcStreamStateValue {
        val version = opaqueStateValue.get("version")?.asInt()
        val isLegacy = MsSqlServerJdbcStreamStateValue.isLegacy(version)

        return if (isLegacy) {
            log.info {
                "Detected legacy state (version=$version), migrating to version ${MsSqlServerJdbcStreamStateValue.CURRENT_VERSION}"
            }
            migrateLegacyState(opaqueStateValue)
        } else {
            try {
                Jsons.treeToValue(opaqueStateValue, MsSqlServerJdbcStreamStateValue::class.java)
            } catch (e: Exception) {
                throw IllegalStateException(
                    "Failed to parse state with version $version as MsSqlServerJdbcStreamStateValue.",
                    e
                )
            }
        }
    }

    private fun migrateLegacyState(
        opaqueStateValue: OpaqueStateValue
    ): MsSqlServerJdbcStreamStateValue {
        val stateType = opaqueStateValue.get("state_type")?.asText()

        return when (stateType) {
            "ordered_column" -> migrateOrderedColumnLoadStatus(opaqueStateValue)
            "cursor_based" -> migrateCursorBasedStatus(opaqueStateValue)
            else -> {
                when {
                    opaqueStateValue.has("ordered_col") ->
                        migrateOrderedColumnLoadStatus(opaqueStateValue)
                    opaqueStateValue.has("cursor_field") ->
                        migrateCursorBasedStatus(opaqueStateValue)
                    else -> {
                        log.warn {
                            "Unknown legacy state format, falling back to default: $opaqueStateValue"
                        }
                        MsSqlServerJdbcStreamStateValue()
                    }
                }
            }
        }
    }

    private fun migrateOrderedColumnLoadStatus(
        opaqueStateValue: OpaqueStateValue
    ): MsSqlServerJdbcStreamStateValue {
        val legacy = Jsons.treeToValue(opaqueStateValue, LegacyOrderedColumnLoadStatus::class.java)

        log.info {
            "Migrating OrderedColumnLoadStatus state: ordered_col=${legacy.orderedCol}, ordered_col_val=${legacy.orderedColVal}"
        }

        val incrementalState =
            legacy.incrementalState
                ?.takeIf { !it.isNull }
                ?.let { migrateCursorBasedStatusFromJson(it) }

        val pkValueNode: JsonNode? =
            legacy.orderedColVal?.let { value ->
                when {
                    value.isEmpty() || value == "null" -> null
                    else -> Jsons.valueToTree<JsonNode>(value)
                }
            }

        return MsSqlServerJdbcStreamStateValue(
            version = MsSqlServerJdbcStreamStateValue.CURRENT_VERSION,
            stateType = StateType.PRIMARY_KEY.stateType,
            pkName = legacy.orderedCol,
            pkValue = pkValueNode,
            incrementalState = incrementalState?.let { Jsons.valueToTree(it) }
        )
    }

    private fun migrateCursorBasedStatusFromJson(
        stateValue: JsonNode
    ): MsSqlServerJdbcStreamStateValue {
        val legacy = Jsons.treeToValue(stateValue, LegacyCursorBasedStatus::class.java)

        log.info {
            "Migrating CursorBasedStatus state: stream=${legacy.streamName}, cursor_field=${legacy.cursorField}, cursor=${legacy.cursor}"
        }

        val cursorNode: JsonNode? =
            legacy.cursor?.let { value ->
                when {
                    value.isEmpty() || value == "null" -> null
                    else -> Jsons.valueToTree<JsonNode>(value)
                }
            }

        return MsSqlServerJdbcStreamStateValue(
            version = MsSqlServerJdbcStreamStateValue.CURRENT_VERSION,
            stateType = StateType.CURSOR_BASED.stateType,
            cursorField = legacy.cursorField ?: emptyList(),
            cursor = cursorNode,
            cursorRecordCount = legacy.cursorRecordCount?.toInt() ?: 0
        )
    }

    private fun migrateCursorBasedStatus(
        opaqueStateValue: OpaqueStateValue
    ): MsSqlServerJdbcStreamStateValue {
        return migrateCursorBasedStatusFromJson(opaqueStateValue)
    }
}
