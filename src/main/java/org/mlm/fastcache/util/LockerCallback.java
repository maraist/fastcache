package org.mlm.fastcache.util;

/**
* Created by IntelliJ IDEA.
* User: aoestreicher
* Date: May 8, 2010
* Time: 2:43:53 PM
* To change this template use File | Settings | File Templates.
*/
public interface LockerCallback<I,R>
{
    R execute(I i);
}
