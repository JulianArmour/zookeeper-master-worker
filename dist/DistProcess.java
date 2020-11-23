import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher , AsyncCallback.ChildrenCallback, AsyncCallback.StringCallback {
  ZooKeeper zk;
  String zkServer, pinfo;
  boolean isMaster = false;

  DistProcess(String zkhost) {
    zkServer = zkhost;
    pinfo = ManagementFactory.getRuntimeMXBean().getName();
    System.out.println("DISTAPP : ZK Connection information : " + zkServer);
    System.out.println("DISTAPP : Process information : " + pinfo);
  }

  void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
    zk = new ZooKeeper(zkServer, 1000, this); //connect to ZK.
    try {
      runForMaster();  // See if you can become the master (i.e, no other master exists)
      isMaster = true;
      // TODO monitor for worker tasks?
    } catch (NodeExistsException nee) {
      isMaster = false;
    }
    System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isMaster ? "master" : "worker"));

    if (isMaster) {
      getTasks(); // Install monitoring on any new tasks that will be created.
    } else {
      //TODO if worker then:
//    Worker worker = new Worker(info...);
//    worker.run()
    }
  }

  // Master fetching task znodes...
  void getTasks() {
    zk.getChildren("/dist25/tasks", this, this, null);
  }

  // Try to become the master.
  void runForMaster() throws UnknownHostException, KeeperException, InterruptedException {
    //Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
    // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
    zk.create("/dist25/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
  }

  public void process(WatchedEvent e) {
    //Get tasks watcher notifications.
    System.out.println("DISTAPP : Event received : " + e);
    getTasks();
  }

  //Asynchronous callback that is invoked by the zk.getChildren request.
  public void processResult(int rc, String path, Object ctx, List<String> children) {
    //TODO: implement pseudocode
//    on cb1(children):
//    for each child in children:
//    createNode(child/handled, handledCB, ctx=child)
  }

  // callback for creating a /handled znode under a task
  @Override
  public void processResult(int rc, String path, Object taskNodeId, String name) {
    //TODO: implement pseudocode
//    on handledCB(ctx=child):
//    if OK:
//    spawn thread TaskDistributor(child)
  }

  public static void main(String[] args) throws Exception {
    //Create a new process
    //Read the ZooKeeper ensemble information from the environment variable.
    DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
    dt.startProcess();

    Object barrier = new Object();
    while (true) {
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
                    CreateMode.PERSISTENT);
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
}
