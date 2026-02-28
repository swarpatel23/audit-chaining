package com.example.reladomo.sql;

import com.example.reladomo.sql.TemporalSqlStore.AuditRow;
import com.example.reladomo.sql.TemporalSqlStore.BitemporalRow;
import com.example.reladomo.util.DateUtils;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DirectSqlChainingTest
{
    private TemporalSqlStore store;

    @Before
    public void setUp() throws Exception
    {
        this.store = TemporalSqlStore.openInMemory("direct_sql_" + System.nanoTime());
    }

    @After
    public void tearDown() throws Exception
    {
        if (this.store != null)
        {
            this.store.close();
        }
    }

    @Test
    public void auditChainingWithDirectSql() throws Exception
    {
        int accountId = 1000;

        this.store.createAuditAccount(accountId, 100.0);
        this.store.incrementAuditBalance(accountId, 200.0);
        this.store.incrementAuditBalance(accountId, 50.0);

        AuditRow current = this.store.findCurrentAuditRow(accountId);
        assertAmountEquals(350.0, current.getBalance());

        List<AuditRow> rows = this.store.fetchAuditRows(accountId);
        assertEquals(3, rows.size());
        assertAmountEquals(100.0, rows.get(0).getBalance());
        assertAmountEquals(300.0, rows.get(1).getBalance());
        assertAmountEquals(350.0, rows.get(2).getBalance());

        assertTrue(rows.get(0).getInZ().before(rows.get(1).getInZ()));
        assertTrue(rows.get(1).getInZ().before(rows.get(2).getInZ()));
        assertTimestampEquals(rows.get(0).getOutZ(), rows.get(1).getInZ());
        assertTimestampEquals(rows.get(1).getOutZ(), rows.get(2).getInZ());
        assertTimestampEquals(rows.get(2).getOutZ(), this.store.getInfinityTimestamp());
    }

    @Test
    public void bitemporalChainingWithDirectSql() throws Exception
    {
        int accountId = 2000;
        Timestamp jan01 = DateUtils.ts("2017-01-01");
        Timestamp jan12 = DateUtils.ts("2017-01-12");
        Timestamp jan17 = DateUtils.ts("2017-01-17");
        Timestamp jan20 = DateUtils.ts("2017-01-20");
        Timestamp infinity = this.store.getInfinityTimestamp();

        this.store.createBitemporalAccount(accountId, jan01, 100.0);
        this.store.incrementBitemporalFrom(accountId, jan20, 200.0);
        this.store.incrementBitemporalUntil(accountId, jan17, jan20, 50.0);

        assertAmountEquals(100.0, this.store.findCurrentBitemporalBalanceAt(accountId, jan12));
        assertAmountEquals(150.0, this.store.findCurrentBitemporalBalanceAt(accountId, jan17));
        assertAmountEquals(300.0, this.store.findCurrentBitemporalBalanceAt(accountId, jan20));

        List<BitemporalRow> current = this.store.fetchCurrentBitemporalRows(accountId);
        assertEquals(3, current.size());
        assertAmountEquals(100.0, current.get(0).getBalance());
        assertAmountEquals(150.0, current.get(1).getBalance());
        assertAmountEquals(300.0, current.get(2).getBalance());
        assertTimestampEquals(jan01, current.get(0).getFromZ());
        assertTimestampEquals(jan17, current.get(0).getThruZ());
        assertTimestampEquals(jan17, current.get(1).getFromZ());
        assertTimestampEquals(jan20, current.get(1).getThruZ());
        assertTimestampEquals(jan20, current.get(2).getFromZ());
        assertTimestampEquals(infinity, current.get(2).getThruZ());
        assertTimestampEquals(current.get(0).getThruZ(), current.get(1).getFromZ());
        assertTimestampEquals(current.get(1).getThruZ(), current.get(2).getFromZ());

        List<BitemporalRow> history = this.store.fetchAllBitemporalRows(accountId);
        assertTrue(history.size() > current.size());
        assertTrue(TemporalSqlStore.hasClosedRows(history, infinity));
        assertTrue(TemporalSqlStore.hasOutMatchedByIn(history, infinity));
        for (BitemporalRow row : history)
        {
            if (!timestampsEqual(row.getOutZ(), infinity))
            {
                assertTrue(row.getOutZ().compareTo(row.getInZ()) >= 0);
            }
        }
    }

    private static void assertAmountEquals(double expected, BigDecimal actual)
    {
        assertEquals(0, actual.compareTo(BigDecimal.valueOf(expected).setScale(4)));
    }

    private static void assertTimestampEquals(Timestamp left, Timestamp right)
    {
        assertEquals(0, left.compareTo(right));
    }

    private static boolean timestampsEqual(Timestamp left, Timestamp right)
    {
        return left.compareTo(right) == 0;
    }
}
