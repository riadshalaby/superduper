package net.rsworld.superduper.schema.liquibase.test;

import java.sql.DriverManager;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;

public final class LiquibaseTestSupport {
    private static final String MASTER_CHANGELOG = "db/changelog/superduper/db.changelog-master.yaml";

    private LiquibaseTestSupport() {}

    public static void migrate(String jdbcUrl, String username, String password) {
        migrate(jdbcUrl, username, password, MASTER_CHANGELOG);
    }

    public static void migrate(String jdbcUrl, String username, String password, String changelog) {
        try (var connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            var database =
                    DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            Liquibase liquibase = new Liquibase(changelog, new ClassLoaderResourceAccessor(), database);
            liquibase.update(new Contexts(), new LabelExpression());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to apply Liquibase changelog: " + changelog, e);
        }
    }
}
