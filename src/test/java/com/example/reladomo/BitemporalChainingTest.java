package com.example.reladomo;

import com.example.reladomo.domain.BitemporalAccount;
import com.example.reladomo.domain.BitemporalAccountFinder;
import com.example.reladomo.util.DateUtils;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.util.DefaultInfinityTimestamp;
import com.gs.fw.finder.Operation;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BitemporalChainingTest extends ReladomoTestBase
{
    @Test
    public void bitemporalChainingAdjustsHistoryByBusinessDate() throws Exception
    {
        int accountId = 2000;

        Timestamp jan01 = DateUtils.ts("2017-01-01");
        Timestamp jan12 = DateUtils.ts("2017-01-12");
        Timestamp jan17 = DateUtils.ts("2017-01-17");
        Timestamp jan20 = DateUtils.ts("2017-01-20");

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            BitemporalAccount account = new BitemporalAccount(jan01);
            account.setAccountId(accountId);
            account.setBalance(100.0);
            account.insert();
            return null;
        });

        Thread.sleep(1100);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            Operation op = BitemporalAccountFinder.accountId().eq(accountId)
                    .and(BitemporalAccountFinder.businessDate().eq(jan20));
            BitemporalAccount account = BitemporalAccountFinder.findOne(op);
            account.setBalance(account.getBalance() + 200.0);
            return null;
        });

        Thread.sleep(1100);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            Operation op = BitemporalAccountFinder.accountId().eq(accountId)
                    .and(BitemporalAccountFinder.businessDate().eq(jan17));
            BitemporalAccount account = BitemporalAccountFinder.findOne(op);
            account.incrementBalanceUntil(50.0, jan20);
            return null;
        });

        BitemporalAccount jan12Account = BitemporalAccountFinder.findOne(
                BitemporalAccountFinder.accountId().eq(accountId)
                        .and(BitemporalAccountFinder.businessDate().eq(jan12)));
        assertEquals(100.0, jan12Account.getBalance(), 0.0001);

        BitemporalAccount jan17Account = BitemporalAccountFinder.findOne(
                BitemporalAccountFinder.accountId().eq(accountId)
                        .and(BitemporalAccountFinder.businessDate().eq(jan17)));
        assertEquals(150.0, jan17Account.getBalance(), 0.0001);

        BitemporalAccount jan20Account = BitemporalAccountFinder.findOne(
                BitemporalAccountFinder.accountId().eq(accountId)
                        .and(BitemporalAccountFinder.businessDate().eq(jan20)));
        assertEquals(300.0, jan20Account.getBalance(), 0.0001);

        Timestamp infinity = DefaultInfinityTimestamp.getDefaultInfinity();
        List<BitemporalRow> currentRows = loadCurrentProcessingRows(accountId, infinity);
        assertEquals(3, currentRows.size());
        assertEquals(100.0, currentRows.get(0).balance, 0.0001);
        assertEquals(150.0, currentRows.get(1).balance, 0.0001);
        assertEquals(300.0, currentRows.get(2).balance, 0.0001);
        assertEquals(jan01.getTime(), currentRows.get(0).fromZ.getTime());
        assertEquals(jan17.getTime(), currentRows.get(0).thruZ.getTime());
        assertEquals(jan17.getTime(), currentRows.get(1).fromZ.getTime());
        assertEquals(jan20.getTime(), currentRows.get(1).thruZ.getTime());
        assertEquals(jan20.getTime(), currentRows.get(2).fromZ.getTime());
        assertEquals(infinity.getTime(), currentRows.get(2).thruZ.getTime());

        List<BitemporalRow> allRows = loadAllRows(accountId);
        assertTrue(allRows.size() > currentRows.size());
        assertTrue(hasAnyClosedRows(allRows, infinity));
        assertTrue(hasOutMatchedByIn(allRows, infinity));
        for (BitemporalRow row : allRows)
        {
            if (!isInfinity(row.outZ, infinity))
            {
                assertTrue(row.outZ.getTime() >= row.inZ.getTime());
            }
        }
    }

    /**
     * After a business-time split (tx2: add 200 at jan20), the unchanged segment [jan01, jan20)
     * must still be queryable by business date. Otherwise "balance as of jan15" would incorrectly
     * return nothing because the row was batched out. This test validates that the current view
     * includes a row for [jan01, jan20) so business-date queries see the correct history.
     */
    @Test
    public void businessDateQuerySeesUnchangedSegmentAfterSplitWithoutCorrection() throws Exception
    {
        int accountId = 2001;

        Timestamp jan01 = DateUtils.ts("2017-01-01");
        Timestamp jan15 = DateUtils.ts("2017-01-15");
        Timestamp jan20 = DateUtils.ts("2017-01-20");

        // Tx1: insert balance 100 from jan01
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            BitemporalAccount account = new BitemporalAccount(jan01);
            account.setAccountId(accountId);
            account.setBalance(100.0);
            account.insert();
            return null;
        });

        Thread.sleep(1100);

        // Tx2: at business date jan20, add 200 (split: [jan01,jan20) stays 100, [jan20,inf) becomes 300). No tx3.
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            Operation op = BitemporalAccountFinder.accountId().eq(accountId)
                    .and(BitemporalAccountFinder.businessDate().eq(jan20));
            BitemporalAccount account = BitemporalAccountFinder.findOne(op);
            account.setBalance(account.getBalance() + 200.0);
            return null;
        });

        // Query by business date jan15 (default processing = now). Should see balance 100.
        BitemporalAccount asOfJan15 = BitemporalAccountFinder.findOne(
                BitemporalAccountFinder.accountId().eq(accountId)
                        .and(BitemporalAccountFinder.businessDate().eq(jan15)));
        assertNotNull("Balance as of business date Jan 15 must be visible (unchanged segment must not be batched out only)", asOfJan15);
        assertEquals("Balance from jan01 to jan20 was 100", 100.0, asOfJan15.getBalance(), 0.0001);

        // Current processing rows (OUT_Z = infinity) must include a row covering [jan01, jan20) with balance 100
        Timestamp infinity = DefaultInfinityTimestamp.getDefaultInfinity();
        List<BitemporalRow> currentRows = loadCurrentProcessingRows(accountId, infinity);
        assertTrue("Current view must have at least 2 rows: [jan01,jan20) 100 and [jan20,inf) 300", currentRows.size() >= 2);
        assertEquals("First current segment must be balance 100 from jan01 to jan20", 100.0, currentRows.get(0).balance, 0.0001);
        assertEquals(jan01.getTime(), currentRows.get(0).fromZ.getTime());
        assertEquals(jan20.getTime(), currentRows.get(0).thruZ.getTime());
        assertEquals("Second current segment must be balance 300 from jan20", 300.0, currentRows.get(1).balance, 0.0001);
        assertEquals(jan20.getTime(), currentRows.get(1).fromZ.getTime());
        assertEquals(infinity.getTime(), currentRows.get(1).thruZ.getTime());
    }

    /**
     * Same as businessDateQuerySeesUnchangedSegmentAfterSplitWithoutCorrection, then add tx3:
     * at business date jan17, incrementBalanceUntil(50, jan20). This corrects the past so
     * [jan17, jan20) becomes 150. Final current view: [jan01,jan17) 100, [jan17,jan20) 150, [jan20,inf) 300.
     */
    @Test
    public void afterTx3CorrectionCurrentViewHasThreeSegments() throws Exception
    {
        int accountId = 2002;

        Timestamp jan01 = DateUtils.ts("2017-01-01");
        Timestamp jan12 = DateUtils.ts("2017-01-12");
        Timestamp jan17 = DateUtils.ts("2017-01-17");
        Timestamp jan20 = DateUtils.ts("2017-01-20");

        // Tx1: insert balance 100 from jan01
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            BitemporalAccount account = new BitemporalAccount(jan01);
            account.setAccountId(accountId);
            account.setBalance(100.0);
            account.insert();
            return null;
        });

        Thread.sleep(1100);

        // Tx2: at business date jan20, add 200
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            Operation op = BitemporalAccountFinder.accountId().eq(accountId)
                    .and(BitemporalAccountFinder.businessDate().eq(jan20));
            BitemporalAccount account = BitemporalAccountFinder.findOne(op);
            account.setBalance(account.getBalance() + 200.0);
            return null;
        });

        Thread.sleep(1100);

        // Tx3: at business date jan17, correct the past: add 50 until jan20 → [jan17,jan20) becomes 150
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            Operation op = BitemporalAccountFinder.accountId().eq(accountId)
                    .and(BitemporalAccountFinder.businessDate().eq(jan17));
            BitemporalAccount account = BitemporalAccountFinder.findOne(op);
            account.incrementBalanceUntil(50.0, jan20);
            return null;
        });

        // Business-date point-in-time queries
        BitemporalAccount asOfJan12 = BitemporalAccountFinder.findOne(
                BitemporalAccountFinder.accountId().eq(accountId)
                        .and(BitemporalAccountFinder.businessDate().eq(jan12)));
        assertNotNull(asOfJan12);
        assertEquals(100.0, asOfJan12.getBalance(), 0.0001);

        BitemporalAccount asOfJan17 = BitemporalAccountFinder.findOne(
                BitemporalAccountFinder.accountId().eq(accountId)
                        .and(BitemporalAccountFinder.businessDate().eq(jan17)));
        assertNotNull(asOfJan17);
        assertEquals(150.0, asOfJan17.getBalance(), 0.0001);

        BitemporalAccount asOfJan20 = BitemporalAccountFinder.findOne(
                BitemporalAccountFinder.accountId().eq(accountId)
                        .and(BitemporalAccountFinder.businessDate().eq(jan20)));
        assertNotNull(asOfJan20);
        assertEquals(300.0, asOfJan20.getBalance(), 0.0001);

        // Current processing rows: exactly 3 segments
        Timestamp infinity = DefaultInfinityTimestamp.getDefaultInfinity();
        List<BitemporalRow> currentRows = loadCurrentProcessingRows(accountId, infinity);
        assertEquals(3, currentRows.size());
        assertEquals(100.0, currentRows.get(0).balance, 0.0001);
        assertEquals(jan01.getTime(), currentRows.get(0).fromZ.getTime());
        assertEquals(jan17.getTime(), currentRows.get(0).thruZ.getTime());
        assertEquals(150.0, currentRows.get(1).balance, 0.0001);
        assertEquals(jan17.getTime(), currentRows.get(1).fromZ.getTime());
        assertEquals(jan20.getTime(), currentRows.get(1).thruZ.getTime());
        assertEquals(300.0, currentRows.get(2).balance, 0.0001);
        assertEquals(jan20.getTime(), currentRows.get(2).fromZ.getTime());
        assertEquals(infinity.getTime(), currentRows.get(2).thruZ.getTime());
    }

    private List<BitemporalRow> loadCurrentProcessingRows(int accountId, Timestamp infinity) throws Exception
    {
        List<BitemporalRow> rows = new ArrayList<>();
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance("test_db");
        try (Connection connection = connectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "select BALANCE, FROM_Z, THRU_Z, IN_Z, OUT_Z from BITEMPORAL_ACCOUNT where ACCOUNT_ID = ? and OUT_Z = ? order by FROM_Z"))
        {
            statement.setInt(1, accountId);
            statement.setTimestamp(2, infinity);
            try (ResultSet resultSet = statement.executeQuery())
            {
                while (resultSet.next())
                {
                    rows.add(new BitemporalRow(
                            resultSet.getDouble(1),
                            resultSet.getTimestamp(2),
                            resultSet.getTimestamp(3),
                            resultSet.getTimestamp(4),
                            resultSet.getTimestamp(5)));
                }
            }
        }
        return rows;
    }

    private List<BitemporalRow> loadAllRows(int accountId) throws Exception
    {
        List<BitemporalRow> rows = new ArrayList<>();
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance("test_db");
        try (Connection connection = connectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "select BALANCE, FROM_Z, THRU_Z, IN_Z, OUT_Z from BITEMPORAL_ACCOUNT where ACCOUNT_ID = ? order by IN_Z, FROM_Z, THRU_Z"))
        {
            statement.setInt(1, accountId);
            try (ResultSet resultSet = statement.executeQuery())
            {
                while (resultSet.next())
                {
                    rows.add(new BitemporalRow(
                            resultSet.getDouble(1),
                            resultSet.getTimestamp(2),
                            resultSet.getTimestamp(3),
                            resultSet.getTimestamp(4),
                            resultSet.getTimestamp(5)));
                }
            }
        }
        return rows;
    }

    private boolean hasAnyClosedRows(List<BitemporalRow> rows, Timestamp infinity)
    {
        for (BitemporalRow row : rows)
        {
            if (!isInfinity(row.outZ, infinity))
            {
                return true;
            }
        }
        return false;
    }

    private boolean hasOutMatchedByIn(List<BitemporalRow> rows, Timestamp infinity)
    {
        Set<Long> inTimes = new HashSet<>();
        for (BitemporalRow row : rows)
        {
            inTimes.add(row.inZ.getTime());
        }
        for (BitemporalRow row : rows)
        {
            if (!isInfinity(row.outZ, infinity) && inTimes.contains(row.outZ.getTime()))
            {
                return true;
            }
        }
        return false;
    }

    private boolean isInfinity(Timestamp timestamp, Timestamp infinity)
    {
        return timestamp.getTime() == infinity.getTime();
    }

    private static class BitemporalRow
    {
        private final double balance;
        private final Timestamp fromZ;
        private final Timestamp thruZ;
        private final Timestamp inZ;
        private final Timestamp outZ;

        private BitemporalRow(double balance, Timestamp fromZ, Timestamp thruZ, Timestamp inZ, Timestamp outZ)
        {
            this.balance = balance;
            this.fromZ = fromZ;
            this.thruZ = thruZ;
            this.inZ = inZ;
            this.outZ = outZ;
        }
    }
}
