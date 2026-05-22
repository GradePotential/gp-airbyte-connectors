/* Copyright (c) 2026 Grade Potential Tutoring. All rights reserved. */
package io.airbyte.integrations.source.mssqlct

import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.StreamIdentifier
import io.airbyte.cdk.check.JdbcCheckQueries
import io.airbyte.cdk.command.SourceConfiguration
import io.airbyte.cdk.discover.Field
import io.airbyte.cdk.discover.JdbcMetadataQuerier
import io.airbyte.cdk.discover.MetadataQuerier
import io.airbyte.cdk.discover.TableName
import io.airbyte.cdk.jdbc.DefaultJdbcConstants
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.read.SelectQueryGenerator
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.airbyte.protocol.models.v0.ConfiguredAirbyteStream
import io.airbyte.protocol.models.v0.StreamDescriptor
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement

private val log = KotlinLogging.logger {}

/** Delegates to [JdbcMetadataQuerier] except for [fields], plus adds CT validation methods. */
class MsSqlSourceMetadataQuerier(
    val base: JdbcMetadataQuerier,
    val configuredCatalog: ConfiguredAirbyteCatalog? = null,
) : MetadataQuerier by base {

    override fun extraChecks() {
        base.extraChecks()
        if (base.config is MsSqlServerSourceConfiguration) {
            val ctConfig = (base.config as MsSqlServerSourceConfiguration)
                .incrementalReplicationConfiguration
            if (ctConfig is ChangeTrackingIncrementalConfiguration) {
                checkDatabaseChangeTrackingEnabled()
            }
        }
    }

    /** Validates that Change Tracking is enabled on the connected database. */
    private fun checkDatabaseChangeTrackingEnabled() {
        // sys.change_tracking_databases contains one row per database that has CT enabled.
        // This view is available on SQL Server 2008+ including Web Edition.
        try {
            base.conn.createStatement().use { stmt: Statement ->
                stmt
                    .executeQuery(
                        "SELECT COUNT(*) AS ct_enabled " +
                            "FROM sys.change_tracking_databases " +
                            "WHERE database_id = DB_ID()"
                    )
                    .use { rs: ResultSet ->
                        rs.next()
                        val ctEnabled = rs.getInt("ct_enabled") > 0
                        if (!ctEnabled) {
                            throw ConfigErrorException(
                                "Change Tracking is not enabled for the database. " +
                                    "Please enable it with: " +
                                    "ALTER DATABASE [YourDatabase] SET CHANGE_TRACKING = ON " +
                                    "(CHANGE_RETENTION = 7 DAYS, AUTO_CLEANUP = ON)"
                            )
                        }
                        log.info { "Change Tracking is enabled on the database." }
                    }
            }
        } catch (e: SQLException) {
            throw ConfigErrorException("Failed to check database Change Tracking status: ${e.message}")
        }
    }

    /**
     * Returns the current Change Tracking version for the connected database.
     * This is used as the baseline version at the start of a snapshot and to advance state
     * after incremental reads.
     */
    fun getCurrentCtVersion(): Long {
        return try {
            base.conn.createStatement().use { stmt: Statement ->
                stmt.executeQuery("SELECT CHANGE_TRACKING_CURRENT_VERSION() AS ct_version")
                    .use { rs: ResultSet ->
                        if (!rs.next()) {
                            throw ConfigErrorException(
                                "Could not retrieve CHANGE_TRACKING_CURRENT_VERSION(). " +
                                    "Ensure Change Tracking is enabled on the database."
                            )
                        }
                        val version = rs.getLong("ct_version")
                        log.info { "Current CT version: $version" }
                        version
                    }
            }
        } catch (e: SQLException) {
            throw ConfigErrorException("Failed to get current CT version: ${e.message}")
        }
    }

    /**
     * Returns the minimum valid CT version for a table (i.e., the oldest version for which
     * CHANGETABLE will still return accurate results). If the stored sync version is older than
     * this, a full re-sync is required.
     */
    fun getMinValidCtVersion(tableObjectId: Long): Long {
        return try {
            base.conn.createStatement().use { stmt: Statement ->
                stmt.executeQuery(
                        "SELECT CHANGE_TRACKING_MIN_VALID_VERSION($tableObjectId) AS min_version"
                    )
                    .use { rs: ResultSet ->
                        if (!rs.next()) return 0L
                        val minVersion = rs.getLong("min_version")
                        if (rs.wasNull()) 0L else minVersion
                    }
            }
        } catch (e: SQLException) {
            log.warn { "Could not get min valid CT version for object $tableObjectId: ${e.message}" }
            0L
        }
    }

    /**
     * Returns the OBJECT_ID for a table (used with CHANGE_TRACKING_MIN_VALID_VERSION).
     * Returns null if the table is not found.
     */
    fun getTableObjectId(schema: String?, table: String): Long? {
        val qualifiedName =
            if (schema != null) "${schema.replace("]", "]]")}.${table.replace("]", "]]")}"
            else table.replace("]", "]]")
        return try {
            base.conn.prepareStatement("SELECT OBJECT_ID(?) AS object_id").use { stmt ->
                stmt.setString(1, qualifiedName)
                stmt.executeQuery().use { rs ->
                    if (!rs.next()) return null
                    val id = rs.getLong("object_id")
                    if (rs.wasNull()) null else id
                }
            }
        } catch (e: SQLException) {
            log.warn { "Could not get OBJECT_ID for $qualifiedName: ${e.message}" }
            null
        }
    }

    /**
     * Returns true if Change Tracking is enabled on the given table.
     * Uses `sys.change_tracking_tables` to determine table-level CT status.
     */
    fun isTableCtEnabled(schema: String?, table: String): Boolean {
        val qualifiedName =
            if (schema != null) "${schema.replace("]", "]]")}.${table.replace("]", "]]")}"
            else table.replace("]", "]]")
        return try {
            base.conn.prepareStatement(
                "SELECT COUNT(*) AS ct FROM sys.change_tracking_tables " +
                    "WHERE object_id = OBJECT_ID(?)"
            ).use { stmt ->
                stmt.setString(1, qualifiedName)
                stmt.executeQuery().use { rs ->
                    rs.next() && rs.getInt("ct") > 0
                }
            }
        } catch (e: SQLException) {
            log.warn { "Could not check CT status for $qualifiedName: ${e.message}" }
            false
        }
    }

    override fun fields(streamID: StreamIdentifier): List<Field> {
        val table: TableName = findTableName(streamID) ?: return listOf()
        if (table !in memoizedColumnMetadata) return listOf()
        return memoizedColumnMetadata[table]!!.map {
            Field(it.label, base.fieldTypeMapper.toFieldType(it))
        }
    }

    override fun streamNamespaces(): List<String> {
        if (base.config.namespaces.isEmpty()) {
            return memoizedTableNames.mapNotNull { it.schema }.distinct()
        }
        return base.config.namespaces.toList()
    }

    val memoizedTableNames: List<TableName> by lazy {
        log.info { "Querying SQL Server table names for catalog discovery." }
        try {
            val allTables = mutableSetOf<TableName>()
            val dbmd = base.conn.metaData
            val currentDatabase = base.conn.catalog

            if (base.config.namespaces.isEmpty()) {
                log.info {
                    "No schemas explicitly configured, discovering all non-system schemas."
                }
                dbmd.getTables(currentDatabase, null, null, null).use { rs ->
                    while (rs.next()) {
                        val schema = rs.getString("TABLE_SCHEM")
                        if (schema != null && !isSystemSchema(schema)) {
                            allTables.add(
                                TableName(
                                    catalog = rs.getString("TABLE_CAT"),
                                    schema = schema,
                                    name = rs.getString("TABLE_NAME"),
                                    type = rs.getString("TABLE_TYPE") ?: "",
                                ),
                            )
                        }
                    }
                }
            } else {
                for (namespace in
                    base.config.namespaces + base.config.namespaces.map { it.uppercase() }) {
                    dbmd.getTables(currentDatabase, namespace, null, null).use { rs ->
                        while (rs.next()) {
                            allTables.add(
                                TableName(
                                    catalog = rs.getString("TABLE_CAT"),
                                    schema = rs.getString("TABLE_SCHEM"),
                                    name = rs.getString("TABLE_NAME"),
                                    type = rs.getString("TABLE_TYPE") ?: "",
                                ),
                            )
                        }
                    }
                }
            }
            log.info {
                "Discovered ${allTables.size} table(s) in SQL Server database '$currentDatabase'."
            }
            return@lazy allTables.toList()
        } catch (e: Exception) {
            throw RuntimeException("SQL Server table discovery query failed: ${e.message}", e)
        }
    }

    val memoizedColumnMetadata: Map<TableName, List<JdbcMetadataQuerier.ColumnMetadata>> by lazy {
        val joinMap: Map<TableName, TableName> =
            memoizedTableNames.associateBy { it.copy(type = "") }
        val results = mutableListOf<Pair<TableName, JdbcMetadataQuerier.ColumnMetadata>>()
        log.info { "Querying SQL Server column names for catalog discovery." }
        try {
            val dbmd = base.conn.metaData
            val currentDatabase = base.conn.catalog

            fun addColumnsFromQuery(
                catalog: String?,
                schema: String?,
                tablePattern: String?,
                isPseudoColumn: Boolean
            ) {
                val rsMethod = if (isPseudoColumn) dbmd::getPseudoColumns else dbmd::getColumns
                rsMethod(catalog, schema, tablePattern, null).use { rs ->
                    while (rs.next()) {
                        val (tableName: TableName, metadata: JdbcMetadataQuerier.ColumnMetadata) =
                            base.columnMetadataFromResultSet(rs, isPseudoColumn)
                        val joinedTableName: TableName = joinMap[tableName] ?: continue
                        results.add(joinedTableName to metadata)
                    }
                }
            }

            if (base.config.namespaces.isEmpty()) {
                log.info { "Querying columns for all schemas." }
                addColumnsFromQuery(currentDatabase, null, null, isPseudoColumn = true)
                addColumnsFromQuery(currentDatabase, null, null, isPseudoColumn = false)
            } else {
                for (namespace in
                    base.config.namespaces + base.config.namespaces.map { it.uppercase() }) {
                    addColumnsFromQuery(currentDatabase, namespace, null, isPseudoColumn = true)
                    addColumnsFromQuery(currentDatabase, namespace, null, isPseudoColumn = false)
                }
            }
            log.info { "Discovered ${results.size} column(s) and pseudo-column(s)." }
        } catch (e: Exception) {
            throw RuntimeException("SQL Server column discovery query failed: ${e.message}", e)
        }
        return@lazy results.groupBy({ it.first }, { it.second }).mapValues {
            (_, columnMetadataByTable) ->
            val deduplicatedColumns = columnMetadataByTable.distinctBy { it.name }
            deduplicatedColumns.filter { it.ordinal == null } +
                deduplicatedColumns.filter { it.ordinal != null }.sortedBy { it.ordinal }
        }
    }

    override fun streamNames(streamNamespace: String?): List<StreamIdentifier> =
        memoizedTableNames
            .filter { it.schema == streamNamespace }
            .map { StreamDescriptor().withName(it.name).withNamespace(streamNamespace) }
            .map(StreamIdentifier::from)

    fun findTableName(streamID: StreamIdentifier): TableName? =
        memoizedTableNames.find { it.name == streamID.name && it.schema == streamID.namespace }

    val memoizedClusteredIndexKeys: Map<TableName, List<List<String>>> by lazy {
        val results = mutableListOf<AllClusteredIndexKeysRow>()
        val schemas: List<String> = streamNamespaces()
        val sql: String = CLUSTERED_INDEX_QUERY_FMTSTR.format(schemas.joinToString { "'$it'" })
        log.info {
            "Querying SQL Server system tables for all clustered index keys for catalog discovery."
        }
        try {
            base.conn.createStatement().use { stmt: Statement ->
                stmt.executeQuery(sql).use { rs: ResultSet ->
                    while (rs.next()) {
                        results.add(
                            AllClusteredIndexKeysRow(
                                rs.getString("table_schema"),
                                rs.getString("table_name"),
                                rs.getString("index_name"),
                                rs.getInt("key_ordinal").takeUnless { rs.wasNull() },
                                rs.getString("column_name").takeUnless { rs.wasNull() },
                            ),
                        )
                    }
                }
            }
            log.info {
                "Discovered all clustered index keys in ${schemas.size} SQL Server schema(s)."
            }
            return@lazy results
                .groupBy {
                    findTableName(
                        StreamIdentifier.from(
                            StreamDescriptor().withName(it.tableName).withNamespace(it.tableSchema),
                        ),
                    )
                }
                .mapNotNull { (table, rowsByTable) ->
                    if (table == null) return@mapNotNull null
                    val clusteredIndexRows: List<AllClusteredIndexKeysRow> =
                        rowsByTable
                            .groupBy { it.indexName }
                            .filterValues { rowsByIndex: List<AllClusteredIndexKeysRow> ->
                                rowsByIndex.all { it.keyOrdinal != null && it.columnName != null }
                            }
                            .values
                            .firstOrNull()
                            ?: return@mapNotNull null
                    val clusteredIndexColumnNames: List<List<String>> =
                        clusteredIndexRows
                            .sortedBy { it.keyOrdinal }
                            .mapNotNull { it.columnName }
                            .map { listOf(it) }
                    table to clusteredIndexColumnNames
                }
                .toMap()
        } catch (e: Exception) {
            throw RuntimeException(
                "SQL Server clustered index discovery query failed: ${e.message}",
                e
            )
        }
    }

    override fun primaryKey(streamID: StreamIdentifier): List<List<String>> {
        val table: TableName = findTableName(streamID) ?: return listOf()

        val databasePK = memoizedPrimaryKeys[table]
        if (!databasePK.isNullOrEmpty()) {
            log.info {
                "Found primary key for table ${table.schema}.${table.name}: ${databasePK.flatten()}"
            }
            return databasePK
        }

        val logicalPK = getUserDefinedPrimaryKey(streamID)
        if (logicalPK.isNotEmpty()) {
            log.info {
                "No physical primary key found for table ${table.schema}.${table.name}. " +
                    "Using user-defined logical primary key: $logicalPK"
            }
            return logicalPK
        }

        log.info { "No primary key or logical PK found for table ${table.schema}.${table.name}" }
        return listOf()
    }

    fun getOrderedColumnForSync(streamID: StreamIdentifier): String? {
        val table: TableName = findTableName(streamID) ?: return null

        val clusteredIndexKeys = memoizedClusteredIndexKeys[table]
        if (clusteredIndexKeys != null && clusteredIndexKeys.size == 1) {
            val column = clusteredIndexKeys[0][0]
            log.info {
                "Using single-column clustered index for sync: ${table.schema}.${table.name} -> $column"
            }
            return column
        }

        val databasePK = memoizedPrimaryKeys[table]
        if (!databasePK.isNullOrEmpty()) {
            val column = databasePK[0][0]
            log.info {
                "Clustered index is composite or not found. Using first PK column: ${table.schema}.${table.name} -> $column"
            }
            return column
        }

        val logicalPK = getUserDefinedPrimaryKey(streamID)
        if (logicalPK.isNotEmpty()) {
            val column = logicalPK[0][0]
            log.info {
                "No physical primary key. Using first logical PK column: ${table.schema}.${table.name} -> $column"
            }
            return column
        }

        log.warn {
            "No suitable column found for ordered column sync: ${table.schema}.${table.name}"
        }
        return null
    }

    private fun getUserDefinedPrimaryKey(streamID: StreamIdentifier): List<List<String>> {
        if (configuredCatalog == null) {
            return listOf()
        }

        val configuredStream: ConfiguredAirbyteStream? =
            configuredCatalog.streams.find {
                it.stream.name == streamID.name && it.stream.namespace == streamID.namespace
            }

        return configuredStream?.primaryKey ?: listOf()
    }

    val memoizedPrimaryKeys: Map<TableName, List<List<String>>> by lazy {
        val results = mutableListOf<AllPrimaryKeysRow>()
        val schemas: List<String> = streamNamespaces()
        val sql: String = PK_QUERY_FMTSTR.format(schemas.joinToString { "'$it'" })
        log.info { "Querying SQL Server system tables for all primary keys for catalog discovery." }
        try {
            base.conn.createStatement().use { stmt: Statement ->
                stmt.executeQuery(sql).use { rs: ResultSet ->
                    while (rs.next()) {
                        results.add(
                            AllPrimaryKeysRow(
                                rs.getString("table_schema"),
                                rs.getString("table_name"),
                                rs.getString("constraint_name"),
                                rs.getInt("ordinal_position").takeUnless { rs.wasNull() },
                                rs.getString("column_name").takeUnless { rs.wasNull() },
                            ),
                        )
                    }
                }
            }
            log.info { "Discovered all primary keys in ${schemas.size} SQL Server schema(s)." }
            return@lazy results
                .groupBy {
                    findTableName(
                        StreamIdentifier.from(
                            StreamDescriptor().withName(it.tableName).withNamespace(it.tableSchema),
                        ),
                    )
                }
                .mapNotNull { (table, rowsByTable) ->
                    if (table == null) return@mapNotNull null
                    val pkRows: List<AllPrimaryKeysRow> =
                        rowsByTable
                            .groupBy { it.constraintName }
                            .filterValues { rowsByPK: List<AllPrimaryKeysRow> ->
                                rowsByPK.all { it.position != null && it.columnName != null }
                            }
                            .values
                            .firstOrNull()
                            ?: return@mapNotNull null
                    val pkColumnNames: List<List<String>> =
                        pkRows
                            .sortedBy { it.position }
                            .mapNotNull { it.columnName }
                            .map { listOf(it) }
                    table to pkColumnNames
                }
                .toMap()
        } catch (e: Exception) {
            throw RuntimeException("SQL Server primary key discovery query failed: ${e.message}", e)
        }
    }

    private data class AllClusteredIndexKeysRow(
        val tableSchema: String,
        val tableName: String,
        val indexName: String,
        val keyOrdinal: Int?,
        val columnName: String?,
    )

    private data class AllPrimaryKeysRow(
        val tableSchema: String,
        val tableName: String,
        val constraintName: String,
        val position: Int?,
        val columnName: String?,
    )

    companion object {

        private val SYSTEM_SCHEMAS =
            setOf(
                "sys",
                "INFORMATION_SCHEMA",
                "cdc",
                "guest",
                "db_accessadmin",
                "db_backupoperator",
                "db_datareader",
                "db_datawriter",
                "db_ddladmin",
                "db_denydatareader",
                "db_denydatawriter",
                "db_owner",
                "db_securityadmin",
                "spt_fallback_db",
                "spt_fallback_dev",
                "spt_fallback_usg",
                "spt_monitor",
                "spt_values",
                "MSreplication_options"
            )

        private fun isSystemSchema(schema: String): Boolean = SYSTEM_SCHEMAS.contains(schema)

        const val CLUSTERED_INDEX_QUERY_FMTSTR =
            """
        SELECT
            s.name as table_schema,
            t.name as table_name,
            i.name as index_name,
            ic.key_ordinal,
            c.name as column_name
        FROM
            sys.tables t
        INNER JOIN
            sys.schemas s ON t.schema_id = s.schema_id
        INNER JOIN
            sys.indexes i ON t.object_id = i.object_id
        INNER JOIN
            sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN
            sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE
            s.name IN (%s)
            AND i.type = 1
            AND i.is_unique = 1
            AND ic.is_included_column = 0
        ORDER BY
            s.name, t.name, ic.key_ordinal;
            """

        const val PK_QUERY_FMTSTR =
            """
        SELECT
            kcu.TABLE_SCHEMA as table_schema,
            kcu.TABLE_NAME as table_name,
            kcu.COLUMN_NAME as column_name,
            kcu.ORDINAL_POSITION as ordinal_position,
            kcu.CONSTRAINT_NAME as constraint_name
        FROM
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        INNER JOIN
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        ON
            kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
        WHERE
            kcu.TABLE_SCHEMA IN (%s)
            AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY';
            """
    }

    /** SQL Server implementation of [MetadataQuerier.Factory]. */
    @Singleton
    @Primary
    class Factory(
        val constants: DefaultJdbcConstants,
        val selectQueryGenerator: SelectQueryGenerator,
        val fieldTypeMapper: JdbcMetadataQuerier.FieldTypeMapper,
        val checkQueries: JdbcCheckQueries,
        val configuredCatalog: ConfiguredAirbyteCatalog? = null,
    ) : MetadataQuerier.Factory<MsSqlServerSourceConfiguration> {
        override fun session(config: MsSqlServerSourceConfiguration): MetadataQuerier {
            val jdbcConnectionFactory = JdbcConnectionFactory(config)
            val base =
                JdbcMetadataQuerier(
                    constants,
                    config,
                    selectQueryGenerator,
                    fieldTypeMapper,
                    checkQueries,
                    jdbcConnectionFactory,
                )
            return MsSqlSourceMetadataQuerier(base, configuredCatalog)
        }
    }
}
