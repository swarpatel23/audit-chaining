package com.example.reladomo.domain;
import java.sql.Timestamp;
public class AuditAccount extends AuditAccountAbstract
{
	public AuditAccount(Timestamp processingDate
	)
	{
		super(processingDate
		);
		// You must not modify this constructor. Mithra calls this internally.
		// You can call this constructor. You can also add new constructors.
	}

	public AuditAccount()
	{
		this(com.gs.fw.common.mithra.util.DefaultInfinityTimestamp.getDefaultInfinity());
	}
}
