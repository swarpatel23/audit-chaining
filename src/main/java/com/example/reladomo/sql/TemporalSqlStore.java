package com.example.reladomo.sql;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Direct-SQL temporal store that models the same chaining semantics
 * demonstrated by the Reladomo audit-only and bitemporal examples.
 */
public class TemporalSqlStore implements AutoCloseable
{
    private static final String H2_USER = "sa";
    private static final String H2_PASSWORD = "";
    private static final Timestamp INFINITY = Timestamp.valueOf("9999-12-31 23:59:59.0");

    private final Connection connection;
    private final MonotonicTimestampGenerator clock;

    public static TemporalSqlStore openInMemory(String databaseName) throws SQLException
    {
        String url = "jdbc:h2:mem:" + databaseName + ";DB_CLOSE_DELAY=-1";
        Connection connection = DriverManager.getConnection(url, H2_USER, H2_PASSWORD);
        return new TemporalSqlStore(connection);
    }

    public TemporalSqlStore(Connection connection) throws SQLException
    {
        this(connection, new MonotonicTimestampGenerator());
    }

    TemporalSqlStore(Connection connection, MonotonicTimestampGenerator clock) throws SQLException
    {
        this.connection = Objects.requireNonNull(connection, "connection must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.connection.setAutoCommit(false);
        this.connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        this.createSchemaIfMissing();
    }

    public Timestamp getInfinityTimestamp()
    {
        return copyTimestamp(INFINITY);
    }

    public void createAuditAccount(int accountId, double openingBalance) throws SQLException
    {
        this.createAuditAccount(accountId, BigDecimal.valueOf(openingBalance));
    }

    public void createAuditAccount(int accountId, BigDecimal openingBalance) throws SQLException
    {
        BigDecimal normalizedOpening = normalizeAmount(openingBalance);
        this.inTransaction(() ->
        {
            AuditRow existing = this.findCurrentAuditRowForUpdate(accountId);
            if (existing != null)
            {
                throw new IllegalStateException("Audit account already exists: " + accountId);
            }
            Timestamp processingTimestamp = this.clock.next();
            this.insertAuditRow(accountId, normalizedOpening, processingTimestamp, INFINITY);
        });
    }

    public void incrementAuditBalance(int accountId, double delta) throws SQLException
    {
        this.incrementAuditBalance(accountId, BigDecimal.valueOf(delta));
    }

    public void incrementAuditBalance(int accountId, BigDecimal delta) throws SQLException
    {
        BigDecimal normalizedDelta = normalizeAmount(delta);
        this.inTransaction(() ->
        {
            AuditRow current = this.findCurrentAuditRowForUpdate(accountId);
            if (current == null)
            {
                throw new IllegalStateException("No active audit row found for account: " + accountId);
            }

            Timestamp processingTimestamp = this.clock.nextAfter(current.inZ);
            BigDecimal nextBalance = current.balance.add(normalizedDelta);
            this.closeAuditRow(accountId, current.inZ, processingTimestamp);
            this.insertAuditRow(accountId, nextBalance, processingTimestamp, INFINITY);
        });
    }

    public void createBitemporalAccount(int accountId, Timestamp businessFrom, double openingBalance) throws SQLException
    {
        this.createBitemporalAccount(accountId, businessFrom, BigDecimal.valueOf(openingBalance));
    }

    public void createBitemporalAccount(int accountId, Timestamp businessFrom, BigDecimal openingBalance) throws SQLException
    {
        Timestamp normalizedBusinessFrom = requiredTimestampCopy(businessFrom, "businessFrom");
        BigDecimal normalizedOpening = normalizeAmount(openingBalance);
        this.inTransaction(() ->
        {
            List<BitemporalRow> existing = this.findCurrentBitemporalRowsForUpdate(accountId);
            if (!existing.isEmpty())
            {
                throw new IllegalStateException("Bitemporal account already exists: " + accountId);
            }

            Timestamp processingTimestamp = this.clock.next();
            this.insertBitemporalRow(
                    accountId,
                    normalizedOpening,
                    normalizedBusinessFrom,
                    INFINITY,
                    processingTimestamp,
                    INFINITY);
        });
    }

    public void incrementBitemporalFrom(int accountId, Timestamp businessFrom, double delta) throws SQLException
    {
        this.incrementBitemporalFrom(accountId, businessFrom, BigDecimal.valueOf(delta));
    }

    public void incrementBitemporalFrom(int accountId, Timestamp businessFrom, BigDecimal delta) throws SQLException
    {
        this.incrementBitemporalUntil(accountId, businessFrom, INFINITY, delta);
    }

    public void incrementBitemporalUntil(int accountId, Timestamp businessFrom, Timestamp businessThruExclusive, double delta) throws SQLException
    {
        this.incrementBitemporalUntil(accountId, businessFrom, businessThruExclusive, BigDecimal.valueOf(delta));
    }

    public void incrementBitemporalUntil(
            int accountId,
            Timestamp businessFrom,
            Timestamp businessThruExclusive,
            BigDecimal delta) throws SQLException
    {
        Timestamp normalizedBusinessFrom = requiredTimestampCopy(businessFrom, "businessFrom");
        Timestamp normalizedBusinessThru = requiredTimestampCopy(businessThruExclusive, "businessThruExclusive");
        BigDecimal normalizedDelta = normalizeAmount(delta);
        if (!isBefore(normalizedBusinessFrom, normalizedBusinessThru))
        {
            throw new IllegalArgumentException("businessFrom must be before businessThruExclusive");
        }

        this.inTransaction(() ->
        {
            List<BitemporalRow> currentRows = this.findCurrentBitemporalRowsForUpdate(accountId);
            if (currentRows.isEmpty())
            {
                throw new IllegalStateException("No active bitemporal rows found for account: " + accountId);
            }

            Timestamp currentStart = currentRows.get(0).fromZ;
            Timestamp currentEnd = currentRows.get(currentRows.size() - 1).thruZ;
            Timestamp processingTimestamp = this.clock.next();

            int closedCount = this.closeCurrentBitemporalRows(accountId, processingTimestamp);
            if (closedCount != currentRows.size())
            {
                throw new IllegalStateException("Concurrent update detected while closing current bitemporal rows");
            }

            List<BusinessSlice> rewritten = this.rewriteBusinessSlices(
                    currentRows,
                    normalizedBusinessFrom,
                    normalizedBusinessThru,
                    normalizedDelta);
            List<BusinessSlice> merged = this.mergeAdjacentSlices(rewritten);
            this.validateSlices(merged, currentStart, currentEnd);

            for (BusinessSlice slice : merged)
            {
                this.insertBitemporalRow(
                        accountId,
                        slice.balance,
                        slice.fromZ,
                        slice.thruZ,
                        processingTimestamp,
                        INFINITY);
            }
        });
    }

    public AuditRow findCurrentAuditRow(int accountId) throws SQLException
    {
        String sql = "select ACCOUNT_ID, BALANCE, IN_Z, OUT_Z from AUDIT_ACCOUNT_SQL " +
                "where ACCOUNT_ID = ? and OUT_Z = ?";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            statement.setTimestamp(2, INFINITY);
            try (ResultSet resultSet = statement.executeQuery())
            {
                if (!resultSet.next())
                {
                    return null;
                }
                return mapAuditRow(resultSet);
            }
        }
    }

    public List<AuditRow> fetchAuditRows(int accountId) throws SQLException
    {
        List<AuditRow> rows = new ArrayList<>();
        String sql = "select ACCOUNT_ID, BALANCE, IN_Z, OUT_Z from AUDIT_ACCOUNT_SQL " +
                "where ACCOUNT_ID = ? order by IN_Z";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            try (ResultSet resultSet = statement.executeQuery())
            {
                while (resultSet.next())
                {
                    rows.add(mapAuditRow(resultSet));
                }
            }
        }
        return rows;
    }

    public BigDecimal findCurrentBitemporalBalanceAt(int accountId, Timestamp businessDate) throws SQLException
    {
        Timestamp normalizedBusinessDate = requiredTimestampCopy(businessDate, "businessDate");
        String sql = "select BALANCE from BITEMPORAL_ACCOUNT_SQL " +
                "where ACCOUNT_ID = ? and OUT_Z = ? and FROM_Z <= ? and THRU_Z > ? " +
                "order by FROM_Z limit 1";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            statement.setTimestamp(2, INFINITY);
            statement.setTimestamp(3, normalizedBusinessDate);
            statement.setTimestamp(4, normalizedBusinessDate);
            try (ResultSet resultSet = statement.executeQuery())
            {
                if (!resultSet.next())
                {
                    return null;
                }
                return resultSet.getBigDecimal(1);
            }
        }
    }

    public List<BitemporalRow> fetchCurrentBitemporalRows(int accountId) throws SQLException
    {
        return this.fetchBitemporalRowsByOutZ(accountId, true);
    }

    public List<BitemporalRow> fetchAllBitemporalRows(int accountId) throws SQLException
    {
        return this.fetchBitemporalRowsByOutZ(accountId, false);
    }

    @Override
    public void close() throws SQLException
    {
        this.connection.close();
    }

    private void createSchemaIfMissing() throws SQLException
    {
        try (PreparedStatement auditTable = this.connection.prepareStatement(
                "create table if not exists AUDIT_ACCOUNT_SQL (" +
                        "ACCOUNT_ID int not null," +
                        "BALANCE decimal(19,4) not null," +
                        "IN_Z timestamp not null," +
                        "OUT_Z timestamp not null," +
                        "primary key (ACCOUNT_ID, IN_Z)," +
                        "check (IN_Z < OUT_Z))");
             PreparedStatement auditIndex = this.connection.prepareStatement(
                     "create index if not exists IDX_AUDIT_ACCOUNT_SQL_CURRENT on AUDIT_ACCOUNT_SQL (ACCOUNT_ID, OUT_Z, IN_Z)");
             PreparedStatement bitemporalTable = this.connection.prepareStatement(
                     "create table if not exists BITEMPORAL_ACCOUNT_SQL (" +
                             "ACCOUNT_ID int not null," +
                             "BALANCE decimal(19,4) not null," +
                             "FROM_Z timestamp not null," +
                             "THRU_Z timestamp not null," +
                             "IN_Z timestamp not null," +
                             "OUT_Z timestamp not null," +
                             "primary key (ACCOUNT_ID, FROM_Z, IN_Z)," +
                             "check (FROM_Z < THRU_Z)," +
                             "check (IN_Z < OUT_Z))");
             PreparedStatement bitemporalIndex = this.connection.prepareStatement(
                     "create index if not exists IDX_BITEMPORAL_ACCOUNT_SQL_CURRENT on BITEMPORAL_ACCOUNT_SQL (ACCOUNT_ID, OUT_Z, FROM_Z, THRU_Z)"))
        {
            auditTable.executeUpdate();
            auditIndex.executeUpdate();
            bitemporalTable.executeUpdate();
            bitemporalIndex.executeUpdate();
            this.connection.commit();
        }
        catch (SQLException e)
        {
            rollbackQuietly();
            throw e;
        }
    }

    private void insertAuditRow(int accountId, BigDecimal balance, Timestamp inZ, Timestamp outZ) throws SQLException
    {
        String sql = "insert into AUDIT_ACCOUNT_SQL (ACCOUNT_ID, BALANCE, IN_Z, OUT_Z) values (?, ?, ?, ?)";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            statement.setBigDecimal(2, balance);
            statement.setTimestamp(3, inZ);
            statement.setTimestamp(4, outZ);
            statement.executeUpdate();
        }
    }

    private void closeAuditRow(int accountId, Timestamp inZ, Timestamp outZ) throws SQLException
    {
        String sql = "update AUDIT_ACCOUNT_SQL set OUT_Z = ? where ACCOUNT_ID = ? and IN_Z = ? and OUT_Z = ?";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setTimestamp(1, outZ);
            statement.setInt(2, accountId);
            statement.setTimestamp(3, inZ);
            statement.setTimestamp(4, INFINITY);
            int updated = statement.executeUpdate();
            if (updated != 1)
            {
                throw new IllegalStateException("Failed to close current audit row for account " + accountId);
            }
        }
    }

    private AuditRow findCurrentAuditRowForUpdate(int accountId) throws SQLException
    {
        String sql = "select ACCOUNT_ID, BALANCE, IN_Z, OUT_Z from AUDIT_ACCOUNT_SQL " +
                "where ACCOUNT_ID = ? and OUT_Z = ? for update";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            statement.setTimestamp(2, INFINITY);
            try (ResultSet resultSet = statement.executeQuery())
            {
                if (!resultSet.next())
                {
                    return null;
                }
                return mapAuditRow(resultSet);
            }
        }
    }

    private void insertBitemporalRow(
            int accountId,
            BigDecimal balance,
            Timestamp fromZ,
            Timestamp thruZ,
            Timestamp inZ,
            Timestamp outZ) throws SQLException
    {
        String sql = "insert into BITEMPORAL_ACCOUNT_SQL (ACCOUNT_ID, BALANCE, FROM_Z, THRU_Z, IN_Z, OUT_Z) " +
                "values (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            statement.setBigDecimal(2, balance);
            statement.setTimestamp(3, fromZ);
            statement.setTimestamp(4, thruZ);
            statement.setTimestamp(5, inZ);
            statement.setTimestamp(6, outZ);
            statement.executeUpdate();
        }
    }

    private List<BitemporalRow> findCurrentBitemporalRowsForUpdate(int accountId) throws SQLException
    {
        List<BitemporalRow> rows = new ArrayList<>();
        String sql = "select ACCOUNT_ID, BALANCE, FROM_Z, THRU_Z, IN_Z, OUT_Z from BITEMPORAL_ACCOUNT_SQL " +
                "where ACCOUNT_ID = ? and OUT_Z = ? order by FROM_Z for update";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            statement.setTimestamp(2, INFINITY);
            try (ResultSet resultSet = statement.executeQuery())
            {
                while (resultSet.next())
                {
                    rows.add(mapBitemporalRow(resultSet));
                }
            }
        }
        return rows;
    }

    private int closeCurrentBitemporalRows(int accountId, Timestamp outZ) throws SQLException
    {
        String sql = "update BITEMPORAL_ACCOUNT_SQL set OUT_Z = ? where ACCOUNT_ID = ? and OUT_Z = ?";
        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setTimestamp(1, outZ);
            statement.setInt(2, accountId);
            statement.setTimestamp(3, INFINITY);
            return statement.executeUpdate();
        }
    }

    private List<BitemporalRow> fetchBitemporalRowsByOutZ(int accountId, boolean onlyCurrent) throws SQLException
    {
        List<BitemporalRow> rows = new ArrayList<>();
        String sql;
        if (onlyCurrent)
        {
            sql = "select ACCOUNT_ID, BALANCE, FROM_Z, THRU_Z, IN_Z, OUT_Z from BITEMPORAL_ACCOUNT_SQL " +
                    "where ACCOUNT_ID = ? and OUT_Z = ? order by FROM_Z";
        }
        else
        {
            sql = "select ACCOUNT_ID, BALANCE, FROM_Z, THRU_Z, IN_Z, OUT_Z from BITEMPORAL_ACCOUNT_SQL " +
                    "where ACCOUNT_ID = ? order by IN_Z, FROM_Z, THRU_Z";
        }

        try (PreparedStatement statement = this.connection.prepareStatement(sql))
        {
            statement.setInt(1, accountId);
            if (onlyCurrent)
            {
                statement.setTimestamp(2, INFINITY);
            }
            try (ResultSet resultSet = statement.executeQuery())
            {
                while (resultSet.next())
                {
                    rows.add(mapBitemporalRow(resultSet));
                }
            }
        }
        return rows;
    }

    private List<BusinessSlice> rewriteBusinessSlices(
            List<BitemporalRow> currentRows,
            Timestamp businessFrom,
            Timestamp businessThru,
            BigDecimal delta)
    {
        List<BusinessSlice> rewritten = new ArrayList<>();
        for (BitemporalRow row : currentRows)
        {
            if (!hasOverlap(row.fromZ, row.thruZ, businessFrom, businessThru))
            {
                rewritten.add(new BusinessSlice(row.fromZ, row.thruZ, row.balance));
                continue;
            }

            Timestamp overlapStart = maxTimestamp(row.fromZ, businessFrom);
            Timestamp overlapEnd = minTimestamp(row.thruZ, businessThru);

            if (isBefore(row.fromZ, overlapStart))
            {
                rewritten.add(new BusinessSlice(row.fromZ, overlapStart, row.balance));
            }
            rewritten.add(new BusinessSlice(overlapStart, overlapEnd, row.balance.add(delta)));
            if (isBefore(overlapEnd, row.thruZ))
            {
                rewritten.add(new BusinessSlice(overlapEnd, row.thruZ, row.balance));
            }
        }
        return rewritten;
    }

    private List<BusinessSlice> mergeAdjacentSlices(List<BusinessSlice> slices)
    {
        if (slices.isEmpty())
        {
            return slices;
        }

        List<BusinessSlice> merged = new ArrayList<>();
        for (BusinessSlice candidate : slices)
        {
            if (merged.isEmpty())
            {
                merged.add(new BusinessSlice(candidate.fromZ, candidate.thruZ, candidate.balance));
                continue;
            }

            BusinessSlice previous = merged.get(merged.size() - 1);
            boolean touchingBoundary = timestampsEqual(previous.thruZ, candidate.fromZ);
            boolean sameBalance = previous.balance.compareTo(candidate.balance) == 0;
            if (touchingBoundary && sameBalance)
            {
                merged.set(
                        merged.size() - 1,
                        new BusinessSlice(previous.fromZ, candidate.thruZ, previous.balance));
            }
            else
            {
                merged.add(new BusinessSlice(candidate.fromZ, candidate.thruZ, candidate.balance));
            }
        }
        return merged;
    }

    private void validateSlices(List<BusinessSlice> slices, Timestamp expectedStart, Timestamp expectedEnd)
    {
        if (slices.isEmpty())
        {
            throw new IllegalStateException("No business slices generated after rewrite");
        }

        if (!timestampsEqual(slices.get(0).fromZ, expectedStart))
        {
            throw new IllegalStateException("Business slices no longer start at the original timeline start");
        }

        if (!timestampsEqual(slices.get(slices.size() - 1).thruZ, expectedEnd))
        {
            throw new IllegalStateException("Business slices no longer end at the original timeline end");
        }

        Timestamp previousThru = null;
        for (BusinessSlice slice : slices)
        {
            if (!isBefore(slice.fromZ, slice.thruZ))
            {
                throw new IllegalStateException("Invalid business slice with non-positive duration");
            }

            if (previousThru != null && isBefore(slice.fromZ, previousThru))
            {
                throw new IllegalStateException("Overlapping business slices detected");
            }
            previousThru = slice.thruZ;
        }
    }

    private AuditRow mapAuditRow(ResultSet resultSet) throws SQLException
    {
        return new AuditRow(
                resultSet.getInt("ACCOUNT_ID"),
                resultSet.getBigDecimal("BALANCE"),
                resultSet.getTimestamp("IN_Z"),
                resultSet.getTimestamp("OUT_Z"));
    }

    private BitemporalRow mapBitemporalRow(ResultSet resultSet) throws SQLException
    {
        return new BitemporalRow(
                resultSet.getInt("ACCOUNT_ID"),
                resultSet.getBigDecimal("BALANCE"),
                resultSet.getTimestamp("FROM_Z"),
                resultSet.getTimestamp("THRU_Z"),
                resultSet.getTimestamp("IN_Z"),
                resultSet.getTimestamp("OUT_Z"));
    }

    private void inTransaction(SqlRunnable runnable) throws SQLException
    {
        try
        {
            runnable.run();
            this.connection.commit();
        }
        catch (SQLException | RuntimeException e)
        {
            rollbackQuietly();
            throw e;
        }
    }

    private void rollbackQuietly()
    {
        try
        {
            this.connection.rollback();
        }
        catch (SQLException ignored)
        {
            // Best-effort rollback only.
        }
    }

    private static BigDecimal normalizeAmount(BigDecimal amount)
    {
        Objects.requireNonNull(amount, "amount must not be null");
        return amount.setScale(4, RoundingMode.HALF_UP);
    }

    private static boolean hasOverlap(Timestamp leftFrom, Timestamp leftThru, Timestamp rightFrom, Timestamp rightThru)
    {
        return isBefore(maxTimestamp(leftFrom, rightFrom), minTimestamp(leftThru, rightThru));
    }

    private static Timestamp maxTimestamp(Timestamp left, Timestamp right)
    {
        return left.compareTo(right) >= 0 ? left : right;
    }

    private static Timestamp minTimestamp(Timestamp left, Timestamp right)
    {
        return left.compareTo(right) <= 0 ? left : right;
    }

    private static boolean isBefore(Timestamp left, Timestamp right)
    {
        return left.compareTo(right) < 0;
    }

    private static boolean timestampsEqual(Timestamp left, Timestamp right)
    {
        return left.compareTo(right) == 0;
    }

    private static Timestamp requiredTimestampCopy(Timestamp timestamp, String fieldName)
    {
        Objects.requireNonNull(timestamp, fieldName + " must not be null");
        return copyTimestamp(timestamp);
    }

    private static Timestamp copyTimestamp(Timestamp timestamp)
    {
        Timestamp copy = new Timestamp(timestamp.getTime());
        copy.setNanos(timestamp.getNanos());
        return copy;
    }

    @FunctionalInterface
    private interface SqlRunnable
    {
        void run() throws SQLException;
    }

    static final class MonotonicTimestampGenerator
    {
        private long lastMillis = Long.MIN_VALUE;

        synchronized Timestamp next()
        {
            long now = System.currentTimeMillis();
            if (now <= this.lastMillis)
            {
                now = this.lastMillis + 1;
            }
            this.lastMillis = now;
            return new Timestamp(now);
        }

        synchronized Timestamp nextAfter(Timestamp floorExclusive)
        {
            long candidate = Math.max(System.currentTimeMillis(), floorExclusive.getTime() + 1);
            if (candidate <= this.lastMillis)
            {
                candidate = this.lastMillis + 1;
            }
            this.lastMillis = candidate;
            return new Timestamp(candidate);
        }
    }

    private static final class BusinessSlice
    {
        private final Timestamp fromZ;
        private final Timestamp thruZ;
        private final BigDecimal balance;

        private BusinessSlice(Timestamp fromZ, Timestamp thruZ, BigDecimal balance)
        {
            this.fromZ = copyTimestamp(fromZ);
            this.thruZ = copyTimestamp(thruZ);
            this.balance = balance;
        }
    }

    public static final class AuditRow
    {
        private final int accountId;
        private final BigDecimal balance;
        private final Timestamp inZ;
        private final Timestamp outZ;

        private AuditRow(int accountId, BigDecimal balance, Timestamp inZ, Timestamp outZ)
        {
            this.accountId = accountId;
            this.balance = balance;
            this.inZ = copyTimestamp(inZ);
            this.outZ = copyTimestamp(outZ);
        }

        public int getAccountId()
        {
            return accountId;
        }

        public BigDecimal getBalance()
        {
            return balance;
        }

        public Timestamp getInZ()
        {
            return copyTimestamp(inZ);
        }

        public Timestamp getOutZ()
        {
            return copyTimestamp(outZ);
        }
    }

    public static final class BitemporalRow
    {
        private final int accountId;
        private final BigDecimal balance;
        private final Timestamp fromZ;
        private final Timestamp thruZ;
        private final Timestamp inZ;
        private final Timestamp outZ;

        private BitemporalRow(
                int accountId,
                BigDecimal balance,
                Timestamp fromZ,
                Timestamp thruZ,
                Timestamp inZ,
                Timestamp outZ)
        {
            this.accountId = accountId;
            this.balance = balance;
            this.fromZ = copyTimestamp(fromZ);
            this.thruZ = copyTimestamp(thruZ);
            this.inZ = copyTimestamp(inZ);
            this.outZ = copyTimestamp(outZ);
        }

        public int getAccountId()
        {
            return accountId;
        }

        public BigDecimal getBalance()
        {
            return balance;
        }

        public Timestamp getFromZ()
        {
            return copyTimestamp(fromZ);
        }

        public Timestamp getThruZ()
        {
            return copyTimestamp(thruZ);
        }

        public Timestamp getInZ()
        {
            return copyTimestamp(inZ);
        }

        public Timestamp getOutZ()
        {
            return copyTimestamp(outZ);
        }
    }

    public static boolean hasClosedRows(List<BitemporalRow> rows, Timestamp infinity)
    {
        for (BitemporalRow row : rows)
        {
            if (!timestampsEqual(row.outZ, infinity))
            {
                return true;
            }
        }
        return false;
    }

    public static boolean hasOutMatchedByIn(List<BitemporalRow> rows, Timestamp infinity)
    {
        Set<Long> inTimes = new HashSet<>();
        for (BitemporalRow row : rows)
        {
            inTimes.add(row.inZ.getTime());
        }

        for (BitemporalRow row : rows)
        {
            if (!timestampsEqual(row.outZ, infinity) && inTimes.contains(row.outZ.getTime()))
            {
                return true;
            }
        }
        return false;
    }
}
