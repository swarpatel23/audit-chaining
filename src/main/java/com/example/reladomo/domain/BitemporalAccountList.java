package com.example.reladomo.domain;
import com.gs.fw.finder.Operation;
import java.util.*;
public class BitemporalAccountList extends BitemporalAccountListAbstract
{
	public BitemporalAccountList()
	{
		super();
	}

	public BitemporalAccountList(int initialSize)
	{
		super(initialSize);
	}

	public BitemporalAccountList(Collection c)
	{
		super(c);
	}

	public BitemporalAccountList(Operation operation)
	{
		super(operation);
	}
}
