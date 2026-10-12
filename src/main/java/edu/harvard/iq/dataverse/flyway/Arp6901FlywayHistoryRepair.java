package edu.harvard.iq.dataverse.flyway;

import org.flywaydb.core.api.FlywayException;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 * Existing ARP databases recorded V6.9.0.1__arp-index-column-names.sql as Flyway
 * version 6.9.0.1. Upstream Dataverse now owns that version. Flyway checks
 * checksums before it runs migrations, so this repair runs first: it applies the
 * upstream script and points the history row at V6.9.0.1.sql.
 */
public final class Arp6901FlywayHistoryRepair {

    private static final Logger logger = Logger.getLogger(Arp6901FlywayHistoryRepair.class.getName());

    static final String OLD_SCRIPT = "V6.9.0.1__arp-index-column-names.sql";
    static final String UPSTREAM_VERSION = "6.9.0.1";
    static final String UPSTREAM_SCRIPT = "V6.9.0.1.sql";
    static final String UPSTREAM_RESOURCE = "db/migration/" + UPSTREAM_SCRIPT;

    private static final String UNDEFINED_TABLE = "42P01";

    private Arp6901FlywayHistoryRepair() {
    }

    public static void repairIfNeeded(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try {
                if (!oldScriptIsRecorded(connection)) {
                    connection.rollback();
                    return;
                }
                String upstreamSql = readUpstreamScript();
                applyUpstreamScript(connection, upstreamSql);
                retargetHistory(connection, checksum(upstreamSql));
                connection.commit();
                logger.info("Retargeted Flyway version " + UPSTREAM_VERSION + " from " + OLD_SCRIPT + " to " + UPSTREAM_SCRIPT);
            } catch (SQLException | IOException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException | IOException e) {
            throw new FlywayException("Failed to retarget Flyway version " + UPSTREAM_VERSION, e);
        }
    }

    private static boolean oldScriptIsRecorded(Connection connection) throws SQLException {
        String sql = "SELECT 1 FROM flyway_schema_history WHERE version = ? AND script = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, UPSTREAM_VERSION);
            statement.setString(2, OLD_SCRIPT);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        } catch (SQLException e) {
            if (UNDEFINED_TABLE.equals(e.getSQLState())) {
                return false;
            }
            throw e;
        }
    }

    private static void applyUpstreamScript(Connection connection, String upstreamSql) throws SQLException {
        for (String part : upstreamSql.split(";")) {
            String statementSql = part.trim();
            if (statementSql.isEmpty() || isOnlyComments(statementSql)) {
                continue;
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute(statementSql);
            }
        }
    }

    private static boolean isOnlyComments(String statementSql) {
        for (String line : statementSql.split("\\R")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("--")) {
                return false;
            }
        }
        return true;
    }

    private static void retargetHistory(Connection connection, int checksum) throws SQLException {
        String sql = "UPDATE flyway_schema_history SET description = '', script = ?, checksum = ? WHERE version = ? AND script = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, UPSTREAM_SCRIPT);
            statement.setInt(2, checksum);
            statement.setString(3, UPSTREAM_VERSION);
            statement.setString(4, OLD_SCRIPT);
            int updated = statement.executeUpdate();
            if (updated != 1) {
                throw new FlywayException("Expected to retarget one Flyway " + UPSTREAM_VERSION + " row, updated " + updated);
            }
        }
    }

    private static String readUpstreamScript() throws IOException {
        try (InputStream in = Arp6901FlywayHistoryRepair.class.getClassLoader().getResourceAsStream(UPSTREAM_RESOURCE)) {
            if (in == null) {
                throw new FlywayException("Missing classpath resource " + UPSTREAM_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Flyway checksums each line without its line ending.
     */
    static int checksum(String sql) throws IOException {
        CRC32 crc32 = new CRC32();
        try (BufferedReader reader = new BufferedReader(new StringReader(sql))) {
            String line;
            while ((line = reader.readLine()) != null) {
                crc32.update(line.getBytes(StandardCharsets.UTF_8));
            }
        }
        return (int) crc32.getValue();
    }
}
