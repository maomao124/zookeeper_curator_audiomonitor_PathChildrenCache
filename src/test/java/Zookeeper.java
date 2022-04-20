import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Project name(项目名称)：zookeeper_curator监听器之PathChildrenCache
 * Package(包名): PACKAGE_NAME
 * Class(类名): Zookeeper
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/4/20
 * Time(创建时间)： 22:40
 * Version(版本): 1.0
 * Description(描述)： 无
 */

public class Zookeeper
{
    private CuratorFramework client;

    @BeforeEach
    void setUp()
    {
        //重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
        //zookeeper创建链接，第一种
                        /*
                        CuratorFramework client =
                                CuratorFrameworkFactory.newClient("127.0.0.1:2181",
                                        60 * 1000,
                                        15 * 1000,
                                        retryPolicy);
                        client.start();
                        */

        //zookeeper创建链接，第二种
        client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("test")
                .build();
        client.start();
    }

    @AfterEach
    void tearDown()
    {
        if (client != null)
        {
            client.close();
        }
    }

    @Test
    void test1() throws Exception
    {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app4", true);

        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener()
        {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent)
                    throws Exception
            {
                System.out.println("子节点已改变");
                byte[] data1 = pathChildrenCacheEvent.getData().getData();
                System.out.println(new String(data1, StandardCharsets.UTF_8));
                System.out.println(pathChildrenCacheEvent);
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                if (type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED))
                {
                    byte[] data = pathChildrenCacheEvent.getData().getData();
                    System.out.println("是更新,更新" + new String(data, StandardCharsets.UTF_8));
                }
            }
        });

        pathChildrenCache.start();

        Scanner input = new Scanner(System.in);
        for (int i = 0; i < 3; i++)
        {
            input.nextLine();
        }
    }
}
