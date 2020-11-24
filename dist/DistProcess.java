import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.List;

public class DistProcess implements Watcher , AsyncCallback.ChildrenCallback, AsyncCallback.StringCallback {
  ZooKeeper zk;
  String zkServer;
  String pinfo;
  boolean isMaster = false;

  DistProcess(String zkhost) {
    zkServer = zkhost;
    pinfo = ManagementFactory.getRuntimeMXBean().getName();
    System.out.println("DISTAPP : ZK Connection information : " + zkServer);
    System.out.println("DISTAPP : Process information : " + pinfo);
  }

  void startProcess() throws IOException, KeeperException, InterruptedException {
    zk = new ZooKeeper(zkServer, 1000, watchedEvent -> {}); //connect to ZK.
    try {
      runForMaster();  // See if you can become the master (i.e, no other master exists)
      isMaster = true;
    } catch (NodeExistsException nee) {
      isMaster = false;
    }
    System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));

    if (isMaster) {
      getTasks(); // Install monitoring on any new tasks that will be created.
    } else {
      Worker worker = new Worker(zk);
      worker.init();
    }
  }

  // Master fetching task znodes...
  void getTasks() {
    zk.getChildren("/dist25/tasks", this, this, null);
  }

  // Try to become the master.
  void runForMaster() throws KeeperException, InterruptedException {
    //Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
    // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
    zk.create("/dist25/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }

  @Override
  public void process(WatchedEvent e) {
    System.out.println("AM I A MASTER???" + isMaster);
    //Get tasks watcher notifications.
    System.out.println("DISTAPP : Event received : " + e);
    getTasks();
  }

  //Asynchronous callback that is invoked by the zk.getChildren request.
  @Override
  public void processResult(int rc, String path, Object ctx, List<String> children) {
    for (String child : children) {
      zk.create("/dist25/tasks/" + child + "/handled", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, this, child);
    }
  }

  // callback for creating a /handled znode under a task
  @Override
  public void processResult(int rc, String path, Object taskNodeId, String name) {
    if (Code.get(rc) == Code.OK){
      TaskDistributor td = new TaskDistributor((String) taskNodeId, zk);
      Thread t = new Thread(td);
      t.start();
    }
  }

  public static void main(String[] args) throws Exception {
    //Create a new process
    //Read the ZooKeeper ensemble information from the environment variable.
    DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
    dt.startProcess();

    Object barrier = new Object();
    //noinspection InfiniteLoopStatement
    while (true) {
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (barrier) {
        barrier.wait();
      }
    }
  }

  private static class TaskDistributor implements Runnable, Watcher {
    private final String taskId;
    private final ZooKeeper zk;
    private boolean workerListChanged = false;

    public TaskDistributor(String taskId, ZooKeeper zk) {
      this.taskId = taskId;
      this.zk = zk;
    }

    List<String> getAvailableWorkers() {
      try {
        return zk.getChildren("/dist25/available_workers", this);
      } catch (KeeperException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return Collections.emptyList();
    }

    private boolean assignTask(List<String> workers) {
      for (String worker : workers) {
        try {
          zk.create("/dist25/worker_tasks/"+worker, taskId.getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
          System.out.println("Assigned "+taskId+" to worker "+worker);
          return true; // successfully assigned the task
        } catch (KeeperException e) {
          // getting Code.NODEEXISTS is normal, but other codes are not
          if (e.code() != Code.NODEEXISTS) {
            e.printStackTrace();
            return false;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      return false; // couldn't assign the task to any of the workers
    }

    @Override
    public void run() {
      System.out.println("Thread="+Thread.currentThread().getName()+" attempting to assign task="+taskId);
      while (true) {
        if (Thread.interrupted()) {
          System.out.println("Master distribution thread="+Thread.currentThread().getName()+" was interrupted while " +
                             "attempting to assign task="+taskId);
          return;
        }
        // get the list of available (idle) workers
        List<String> availableWorkers = getAvailableWorkers();
        // try to assign a task to a worker
        boolean success = assignTask(availableWorkers);
        if (success) {
          return;
        }
        //couldn't assign the task, wait for notification from watch to try again
        System.out.println("Master distribution thread="+Thread.currentThread().getName()+"Couldn't assign task yet," +
                           " retrying when worker list changes.");
        synchronized (this) {
          try {
            while (!workerListChanged) {
              this.wait();
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          workerListChanged = false;
        }
      }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      synchronized (this) {
        workerListChanged = true;
        this.notifyAll();
      }
    }
  }

  private static class Worker implements Watcher, DataCallback, StatCallback {
    private final ZooKeeper zk;
    private String workerId;

    public Worker(ZooKeeper zk) {
      this.zk = zk;
    }

    public void init() {
      try {
        String workerPath = zk.create("/dist25/available_workers/worker-", "".getBytes(), Ids.OPEN_ACL_UNSAFE,
                      CreateMode.EPHEMERAL_SEQUENTIAL);
        workerId = workerPath.replace("/dist25/available_workers/", "");
        System.out.println(workerId + " ready for task");
        System.out.println("exists() on /dist25/worker_tasks/"+workerId);
        zk.exists("/dist25/worker_tasks/" + workerId, this, this, null);
      } catch (KeeperException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] taskIdBytes, Stat stat) {
      System.out.println("result from getData on /dist25/worker_tasks/"+workerId);
      if (Code.get(rc) == Code.NONODE) {
        System.out.println("/dist25/worker_tasks/"+workerId+" doesn't exist yet");
        return;
      }
      System.out.println("/dist25/worker_tasks/"+workerId+" getData success");
      System.out.println("deleting "+"/dist25/available_workers/"+workerId);
      zk.delete("/dist25/available_workers/"+workerId, -1, null, null);
      //execute work in another thread
      Thread executor = new Thread(() -> {
        String taskId = new String(taskIdBytes);
        try {
          // get task
          byte[] taskSerial = zk.getData("/dist25/tasks/"+taskId, false, null);
          //compute
          ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
          ObjectInput in = new ObjectInputStream(bis);
          DistTask dt = (DistTask) in.readObject();
          dt.compute();
          ByteArrayOutputStream bos = new ByteArrayOutputStream();
          ObjectOutputStream oos = new ObjectOutputStream(bos);
          oos.writeObject(dt);
          oos.flush();
          taskSerial = bos.toByteArray();
          //write back result
          zk.create("/dist25/tasks/" + taskId + "/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
          if (e.code() != Code.NONODE)
            e.printStackTrace();
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
          e.printStackTrace();
        } finally {
          //reset worker
          zk.delete("/dist25/worker_tasks/"+workerId, -1, null, null);
          init();
        }
      });
      executor.start();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
      System.out.println("Watch triggered on exist() /dist25/worker_tasks/"+workerId);
      zk.getData("/dist25/worker_tasks/"+workerId, false, this, null);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (Code.get(rc) == Code.OK)
        zk.getData(path, false, this, null);
    }
  }
}
