import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Main implements Watcher{

    ZooKeeper zk;
    String hostAddress = "localhost:2181";
    String nodeId = Integer.toHexString(new Random().nextInt());
    boolean isLeader = false;
    String nodePath;

    String vote;

    @Override
    public void process(WatchedEvent watchedEvent) {
        //ako je dosao novi glas (a lider sam)
        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged && isLeader) {
            //pokupi sve votere
            try {
                List<String> voters = zk.getChildren("/votes", true);
                if (voters.size() >= 3) {   //==
                    int yesVote = 0;
                    int noVote = 0;
                    //povadi i prebroj glasove
                    for (String voter : voters) {
                        String vote = new String(zk.getData("/votes/" + voter, false, null));
                        if (vote.equals("yes")) {
                            yesVote++;
                        } else {
                            noVote++;
                        }
                    }
                    String result = yesVote > noVote ? "yes" : "no";
                    Stat leaderStat = zk.exists("/leader", false);
                    int leaderVersion = leaderStat.getVersion();
                    zk.setData("/leader", result.getBytes(), leaderVersion);    //upisi rezultat u /leader data
                    System.out.println("Rezultat glasanja: " + result);
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged && !isLeader) {
            try {
                if (new String(zk.getData("/leader", false, null)).equals(vote)) {
                    System.out.println(nodeId + ": Moj glas je pobedio! (" + vote + ")" );
                } else {
                    System.out.println(nodeId + ": Moj glas je izgubio...(" + vote + ")");
                }
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    void start() throws IOException {
        zk = new ZooKeeper(hostAddress, 2000, this);
    }

    void stop() throws InterruptedException {
        zk.close();
    }

    void runForLeader() throws KeeperException, InterruptedException {
        try {
            nodePath = zk.create("/leader", "start".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);  //nodeId.getBytes()
            isLeader = true;
            zk.getChildren("/votes", true); //leader stavlja watcher na /votes cvor
        } catch (KeeperException.NodeExistsException e) {
            isLeader = false;
            vote();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();    //nebitni exceptioni u ovom slucaju
        } finally {
//            System.out.println(nodeId + " is leader: " + isLeader);
        }
    }

    void vote() throws KeeperException, InterruptedException {
        if (new String(zk.getData("/leader", true, null)).equals("start")) {
            Random random = new Random();
            vote = random.nextBoolean() ? "yes" : "no";
            nodePath  = zk.create("/votes/vote-", vote.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Logger.getRootLogger().setLevel(Level.OFF);

        List<Main> clients = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            clients.add(new Main());
        }

        for (Main client : clients) {
            client.start();
            client.runForLeader();
        }

        Thread.sleep(60000);
        for (Main client : clients) {
            client.stop();
        }
    }


}
