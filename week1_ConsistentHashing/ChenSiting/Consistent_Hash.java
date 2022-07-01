import java.util.*;

public class Consistent_Hash {//构造
    private static final int VIRTUAL_MACHINE_NUMBER = 5;//设定每个物理服务器开设多少虚拟节点
    private SortedMap<Integer, String> virtualNodeMap = new TreeMap<Integer, String>();//虚拟节点
    private List<String> trueNodeList = new ArrayList<String>();//物理服务器节点

    public void addNode(String node) {//添加物理服务器
        if (trueNodeList.contains(node)) {//存在则返回
            return;
        }
        trueNodeList.add(node);//新节点加入
        for (int i = 0; i < VIRTUAL_MACHINE_NUMBER; i++) {//分配对应虚拟节点
            String virtualNodeName = node + i;
            int hash = getHash(virtualNodeName);//计算虚拟节点的哈希值
            virtualNodeMap.put(hash, virtualNodeName);
        }
    }


    public void delNode(String node) {//删除节点
        trueNodeList.remove(node);//删除物理服务器节点
        for (int i = 0; i < VIRTUAL_MACHINE_NUMBER; i++) {//删除对应虚拟节点
            String virtualNodeName = node + i;
            int hash = getHash(virtualNodeName);
            virtualNodeMap.remove(hash);
        }
    }

    public int getHash(String key) {//计算哈希值
        int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < key.length(); i++) {
            hash = (hash ^ key.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return hash > 0 ? hash : Math.abs(hash);
    }

    public String getServer(String key) {//申请服务器节点
        int hash = getHash(key);
        SortedMap<Integer, String> tailMap = virtualNodeMap.tailMap(hash);
        int nodeKey;
        if (tailMap.isEmpty()) {
            nodeKey = virtualNodeMap.firstKey();
        } else {
            nodeKey = tailMap.firstKey();
        }
        String virtualNodeName = virtualNodeMap.get(nodeKey);
        return virtualNodeName.substring(0, virtualNodeName.length() - 1);
    }

    public static void main(String[] args) {
        Consistent_Hash Consistent_Hash = new Consistent_Hash();
        String[] nodes = {"192.168.1.1", "192.168.1.2", "192.168.1.3"};//3台物理服务器
        for (String node : nodes) {
            Consistent_Hash.addNode(node);
        }

        Map<String, List<Integer>> taskDistributionMap = new HashMap();

        for (int i = 0; i < 1000; i++) {//方便演示，0-100作为要缓存的内容，实际需要更换函数
            String server = Consistent_Hash.getServer("abc" + i + "def");
            List<Integer> list = taskDistributionMap.get(server);
            if (list == null) {
                list = new ArrayList<>();
                taskDistributionMap.put(server, list);
            }
            list.add(i);
        }

        for (Map.Entry<String, List<Integer>> entry : taskDistributionMap.entrySet()) {
            System.out.print("物理服务器地址：" + entry.getKey() + " " + " 缓存总量：" + entry.getValue().size() + " 缓存内容:");
            for (int t : entry.getValue()) {
                System.out.print(t + " ");
            }
            System.out.println();
        }

        System.out.println("===================================================");

        Consistent_Hash.addNode("192.168.1.4");//增添一个物理服务器节点对比查看
        taskDistributionMap = new HashMap();

        for (int i = 0; i < 1000; i++) {
            String server = Consistent_Hash.getServer("abc" + i + "def");
            List<Integer> list = taskDistributionMap.get(server);
            if (list == null) {
                list = new ArrayList<>();
                taskDistributionMap.put(server, list);
            }
            list.add(i);
        }
        for (Map.Entry<String, List<Integer>> entry : taskDistributionMap.entrySet()) {
            System.out.print("物理服务器地址：" + entry.getKey() + " " + " 缓存总量：" + entry.getValue().size() + " 缓存内容:");
            for (int t : entry.getValue()) {
                System.out.print(t + " ");
            }
            System.out.println();
        }
    }
}
