package com.example.reladomo;

import com.example.reladomo.domain.AuditAccount;
import com.example.reladomo.domain.AuditAccountFinder;
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
import static org.junit.Assert.assertTrue;

public class AuditOnlyChainingTest extends ReladomoTestBase
{
    @Test
    public void auditOnlyChainingCreatesHistory() throws Exception
    {
        int accountId = 1000;

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            AuditAccount account = new AuditAccount();
            account.setAccountId(accountId);
            account.setBalance(100.0);
            account.insert();
            return null;
        });

        Thread.sleep(1100);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            Operation op = AuditAccountFinder.accountId().eq(accountId);
            AuditAccount account = AuditAccountFinder.findOne(op);
            account.setBalance(account.getBalance() + 200.0);
            return null;
        });

        Thread.sleep(1100);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(tx ->
        {
            Operation op = AuditAccountFinder.accountId().eq(accountId);
            AuditAccount account = AuditAccountFinder.findOne(op);
            account.setBalance(account.getBalance() + 50.0);
            return null;
        });

        AuditAccount latest = AuditAccountFinder.findOne(AuditAccountFinder.accountId().eq(accountId));
        assertEquals(350.0, latest.getBalance(), 0.0001);

        List<AuditRow> rows = loadAuditRows(accountId);
        assertEquals(3, rows.size());

        Set<Double> balances = new HashSet<>();
        for (AuditRow row : rows)
        {
            balances.add(row.balance);
        }
        assertTrue(balances.contains(100.0));
        assertTrue(balances.contains(300.0));
        assertTrue(balances.contains(350.0));

        assertTrue(rows.get(0).inZ.before(rows.get(1).inZ));
        assertTrue(rows.get(1).inZ.before(rows.get(2).inZ));
        assertEquals(rows.get(1).inZ.getTime(), rows.get(0).outZ.getTime());
        assertEquals(rows.get(2).inZ.getTime(), rows.get(1).outZ.getTime());

        Timestamp infinity = DefaultInfinityTimestamp.getDefaultInfinity();
        assertEquals(infinity.getTime(), rows.get(2).outZ.getTime());
    }

    private List<AuditRow> loadAuditRows(int accountId) throws Exception
    {
        List<AuditRow> rows = new ArrayList<>();
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance("test_db");
        try (Connection connection = connectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "select BALANCE, IN_Z, OUT_Z from AUDIT_ACCOUNT where ACCOUNT_ID = ? order by IN_Z"))
        {
            statement.setInt(1, accountId);
            try (ResultSet resultSet = statement.executeQuery())
            {
                while (resultSet.next())
                {
                    rows.add(new AuditRow(
                            resultSet.getDouble(1),
                            resultSet.getTimestamp(2),
                            resultSet.getTimestamp(3)));
                }
            }
        }
        return rows;
    }

    private static class AuditRow
    {
        private final double balance;
        private final Timestamp inZ;
        private final Timestamp outZ;

        private AuditRow(double balance, Timestamp inZ, Timestamp outZ)
        {
            this.balance = balance;
            this.inZ = inZ;
            this.outZ = outZ;
        }
    }
}
