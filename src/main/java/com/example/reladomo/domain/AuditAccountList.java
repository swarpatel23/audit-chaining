package com.example.reladomo.domain;
import com.gs.fw.finder.Operation;
import java.util.*;
public class AuditAccountList extends AuditAccountListAbstract
{
	public AuditAccountList()
	{
		super();
	}

	public AuditAccountList(int initialSize)
	{
		super(initialSize);
	}

	public AuditAccountList(Collection c)
	{
		super(c);
	}

	public AuditAccountList(Operation operation)
	{
		super(operation);
	}
}
