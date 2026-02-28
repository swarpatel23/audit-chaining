package com.example.reladomo;

import com.gs.fw.common.mithra.test.ConnectionManagerForTests;
import com.gs.fw.common.mithra.test.MithraTestResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class ReladomoTestBase
{
    protected static MithraTestResource testResource;

    @BeforeClass
    public static void setUpReladomo() throws Exception
    {
        if (testResource != null)
        {
            return;
        }
        testResource = new MithraTestResource("testconfig/ReladomoTestRuntimeConfiguration.xml");
        ConnectionManagerForTests connectionManager = ConnectionManagerForTests.getInstance("test_db");
        testResource.createSingleDatabase(connectionManager, "testdata/ReladomoTestData.txt");
        testResource.setUp();
    }

    @AfterClass
    public static void tearDownReladomo() throws Exception
    {
        if (testResource != null)
        {
            testResource.tearDown();
            testResource = null;
        }
    }
}
