# Alpenhorn Demo

## Introduction

This is a short demonstration of using alpenhorn intended to show off most of
the major features of the system.

## Demo Set-up

Because alpenhorn is designed to run as a distributed system, with both data and
software present at multiple dispersed, independent places, this demo uses
[docker](https://docs.docker.com/) to run several virtual images simulating
independent hosts.  Additionally, [Docker Compose](https://docs.docker.com/compose/)
is used to manage the multi-container set-up for this demo.

Installing docker itself is beyond the scope of this demo.  The
[Docker install documentation](https://docs.docker.com/get-started/get-docker/) may
help.  You may also be able to get help from your friendly neighbourhood sysadmin.
Once docker is properly installed you can test the installation by running the `hello-world`
container:
```(console)
$ docker run hello-world
Unable to find image 'hello-world:latest' locally
latest: Pulling from library/hello-world
e6590344b1a5: Pull complete
Digest: sha256:d715f14f9eca81473d9112df50457893aa4d099adeb4729f679006bf5ea12407
Status: Downloaded newer image for hello-world:latest

Hello from Docker!
This message shows that your installation appears to be working correctly.

To generate this message, Docker took the following steps:
 1. The Docker client contacted the Docker daemon.
 2. The Docker daemon pulled the "hello-world" image from the Docker Hub.
    (amd64)
 3. The Docker daemon created a new container from that image which runs the
    executable that produces the output you are currently reading.
 4. The Docker daemon streamed that output to the Docker client, which sent it
    to your terminal.

To try something more ambitious, you can run an Ubuntu container with:
 $ docker run -it ubuntu bash

Share images, automate workflows, and more with a free Docker ID:
 https://hub.docker.com/

For more examples and ideas, visit:
 https://docs.docker.com/get-started/
```

Once docker itself is running, you'll also need to
[install the docker compose plugin](https://docs.docker.com/compose/install/linux/).
Afer installing docker compose, you should be able to run
```
  docker compose version
```
and see the version of the plugin you just installed:
```(console)
$ docker compose version
Docker Compose version v2.31.0
```

If that's all working, you should be able to proceed with the alpenhorn demo itself!

## Starting the demo database

There are four docker containers that comprise this demo:
* a database container, which runs the MySQL server containing the alpenhorn Data Index
* three containers implementing the separate alpenhorn hosts, each containing a StorageNodes

These containers will be automatically built when we first start the demo.

The demo must be run from the `/demo/` subdirectory.  (The directory containing this file.)

Let's start off by starting the database container in the background.  Because alpenhorn
is a distributed system, it is not expected that the database itself runs on an alpenhorn
node.  We simulate this in the demo by running the database out of a standard mysql container.

To start the database container, run the following from the `/demo` subdirectory:
```
    docker compose up --detach alpdb
```

If you get a `no configuration file provided: not found` error, you're not in the right
directory.  (The directory also should have the `Dockerfile.alpenhorn` file and the
`docker-compose.yaml` which came with this demo.)

Doing this the first time will probably cause docker to download the latest MySQL image:
```(console)
$ docker compose up --detach alpdb
[+] Running 11/11
 ✔ alpdb Pulled                                                             15.9s
 ✔ 1d19e87a21f5 Pull complete                                                3.0s
 ✔ 16ec22ff04f9 Pull complete                                                3.1s
 ✔ 9f789b8d2675 Pull complete                                                3.1s
 ✔ 96f4da41c548 Pull complete                                                3.5s
 ✔ fb087646189b Pull complete                                                3.5s
 ✔ 023374826adc Pull complete                                                3.5s
 ✔ 8293a632aa25 Pull complete                                                4.6s
 ✔ c3947540e0c6 Pull complete                                                4.7s
 ✔ c38bed95fb4b Pull complete                                               14.5s
 ✔ 712eb897f1e5 Pull complete                                               14.5s
[+] Running 1/1
 ✔ Container demo-alpdb-1  Started 
```

You can use `docker status` or `docker container ls` to verify that the alpdb container is running:
```(console)
$ docker container ls
CONTAINER ID   IMAGE          COMMAND         CREATED         STATUS         PORTS                 NAMES
7e19895eb701   mysql:latest   "docker-ent…"   5 minutes ago   Up 5 minutes   3306/tcp, 33060/tcp   demo-alpdb-1
```

## Stopping and resetting the demo

Before we continue, a few words about stopping and resetting this demo.

You can stop the docker containers running this demo at any time by executing:
```
docker compose stop
```
This will stop all running containers.  To restar the demo, run the appropriate `docker compose up` commands.
Stopping the demo does not delete the containers or volumes containing the database and the storage node data.

If you want to also remove the demo containers:
```
docker compose down --remove-orphans
```

To remove the containers _and_ the volumes containing the database and the storage node data:
```
docker compose down --remove-orphans --volumes
```
Doing this will require rebuilding the database as described below.

Finally, to remove the alpenhorn container image which gets built the first time the containers is
run:
```
docker rmi alpenhorn:latest
```
You should do this if you want to update the alpenhorn version used by the demo, or if you've
made changes to the demo's `Dockerfile.alpenhorn` or `docker-compose.yaml` files.  (You can
also remove the `mysql:latest` image if you want to run a newer version of the database
container.)


## Initialising the database

Now we need to use some `alpenhorn` commands to create the Data Index (the alpenhorn database)
and the storage infrastructure in it.  The data index must exist before we can start the first
alpenhorn container.

To create the data index we'll need access to the database.  We'll do that by creating a
temporary container running the image from the first alpenhorn node (`alpen1`):

To build the container and start a bash session in it, run:
```
docker compose run --rm alpen1 bash -l
```

Running this the first time will cause docker compose to build the `alpenhorn` docker image.
This may take some time.  Eventually you should be presented with a bash prompt as root inside
the `alpen1` container:
```(console)
$ docker compose run --rm alpen1 bash -l
[+] Creating 3/0
 ✔ Volume "demo_node1_vol"      Created                                                                                                                                                                                 0.0s 
 ✔ Volume "demo_transport_vol"  Created                                                                                                                                                                                 0.0s 
 ✔ Container demo-alpdb-1       Running                                                                                                                                                                                 0.0s 
[+] Running 1/1
 ! alpen1 Warning pull access denied for alpenhorn, repository does not exist or may require 'docker login': denied: requested access to the resource is denied                                                          0.9s 
[+] Building 78.4s (17/17) FINISHED                                                                                                                                                                           docker:default
 => [alpen1 internal] load build definition from Dockerfile.alpenhorn                                                                                                                                                   0.0s
 => => transferring dockerfile: 1.20kB                                                                                                                                                                                  0.0s
 => [alpen1 internal] load metadata for docker.io/library/python:3.11                                                                                                                                                   1.2s
 => [alpen1 internal] load .dockerignore                                                                                                                                                                                0.0s
 => => transferring context: 2B                                                                                                                                                                                         0.0s
 => [alpen1 internal] load build context                                                                                                                                                                                0.2s
 => => transferring context: 2.89MB                                                                                                                                                                                     0.2s
 => [alpen1  1/11] FROM docker.io/library/python:3.11@sha256:14b4620f59a90f163dfa6bd252b68743f9a41d494a9fde935f9d7669d98094bb                                                                                          18.8s
 => => resolve docker.io/library/python:3.11@sha256:14b4620f59a90f163dfa6bd252b68743f9a41d494a9fde935f9d7669d98094bb                                                                                                    0.0s
 => => sha256:14b4620f59a90f163dfa6bd252b68743f9a41d494a9fde935f9d7669d98094bb 9.08kB / 9.08kB                                                                                                                          0.0s
 => => sha256:fa951df28e3fef5b5736bf5d0c285f91e7c8d1c814bfc3784c1a4b3d216b39ee 2.33kB / 2.33kB                                                                                                                          0.0s
 => => sha256:35af2a7690f2b43e7237d1fae8e3f2350dfb25f3249e9cf65121866f9c56c772 64.39MB / 64.39MB                                                                                                                        1.7s
 => => sha256:78a74fb73bfb12a8641cc50cbc82f57c610aaafa73b628896cb71a475497922c 6.18kB / 6.18kB                                                                                                                          0.0s
 => => sha256:a492eee5e55976c7d3feecce4c564aaf6f14fb07fdc5019d06f4154eddc93fde 48.48MB / 48.48MB                                                                                                                        1.8s
 => => sha256:32b550be6cb62359a0f3a96bc0dc289f8b45d097eaad275887f163c6780b4108 24.06MB / 24.06MB                                                                                                                        1.3s
 => => sha256:7576b00d9bb10cc967bb5bdeeb3d5fa078ac8800e112aa03ed15ec199662d4f7 211.33MB / 211.33MB                                                                                                                      8.0s
 => => sha256:3fd67c6ea72187077ad551000d527ae7c24d461e7c9944dc74312e3afac50fb4 6.16MB / 6.16MB                                                                                                                          2.1s
 => => sha256:dcaa1b9153e7d7edb7678e2ef9933b57b19a97ec9bce49dc8630911aa18664d1 24.31MB / 24.31MB                                                                                                                        2.8s
 => => extracting sha256:a492eee5e55976c7d3feecce4c564aaf6f14fb07fdc5019d06f4154eddc93fde                                                                                                                               2.7s
 => => sha256:8630e3071c887d36db54d1924e21eee3419c5afc9874a068e03fb7978b2fb7d8 249B / 249B                                                                                                                              2.3s
 => => extracting sha256:32b550be6cb62359a0f3a96bc0dc289f8b45d097eaad275887f163c6780b4108                                                                                                                               0.7s
 => => extracting sha256:35af2a7690f2b43e7237d1fae8e3f2350dfb25f3249e9cf65121866f9c56c772                                                                                                                               3.2s
 => => extracting sha256:7576b00d9bb10cc967bb5bdeeb3d5fa078ac8800e112aa03ed15ec199662d4f7                                                                                                                               7.8s
 => => extracting sha256:3fd67c6ea72187077ad551000d527ae7c24d461e7c9944dc74312e3afac50fb4                                                                                                                               0.4s
 => => extracting sha256:dcaa1b9153e7d7edb7678e2ef9933b57b19a97ec9bce49dc8630911aa18664d1                                                                                                                               1.1s
 => => extracting sha256:8630e3071c887d36db54d1924e21eee3419c5afc9874a068e03fb7978b2fb7d8                                                                                                                               0.0s
 => [alpen1  2/11] RUN apt-get update && apt-get install --no-install-recommends -y     vim     ssh     rsync     netcat-openbsd     default-mysql-client                                                              13.2s
 => [alpen1  3/11] RUN pip install --no-cache-dir mysqlclient                                                                                                                                                           8.3s 
 => [alpen1  4/11] RUN ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa                                                                                                                                                     2.2s 
 => [alpen1  5/11] RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys                                                                                                                                              0.6s 
 => [alpen1  6/11] RUN echo 'Host *\n    StrictHostKeyChecking no\n' > /root/.ssh/config                                                                                                                                0.6s 
 => [alpen1  7/11] COPY demo/alpenhorn.conf /etc/alpenhorn/alpenhorn.conf                                                                                                                                               0.1s 
 => [alpen1  8/11] RUN mkdir /var/log/alpenhorn                                                                                                                                                                         0.5s 
 => [alpen1  9/11] COPY examples/pattern_importer.py /root/python/pattern_importer.py                                                                                                                                   0.1s 
 => [alpen1 10/11] ADD . /build                                                                                                                                                                                         0.2s
 => [alpen1 11/11] RUN cd /build && pip install .                                                                                                                                                                      31.3s
 => [alpen1] exporting to image                                                                                                                                                                                         1.1s 
 => => exporting layers                                                                                                                                                                                                 1.0s 
 => => writing image sha256:cbbb72cb54b858da771eae6590640747fc884282239056c127a5881f008532a3                                                                                                                            0.0s 
 => => naming to docker.io/library/alpenhorn                                                                                                                                                                            0.0s 
 => [alpen1] resolving provenance for metadata file                                                                                                                                                                     0.0s 
root@alpen1:/#
```

Once at the root prompt, we can build the data index and start populating it.

### Setting up the import extension

Because alpenhorn is data agnostic, it doesn't have any facilities out-of-the-box to import files.
To be able to import files, alpenhorn needs one or more "import-detect extensions" to be loaded.
For the purposes of this demo, we use the simple `pattern_importer` example import-detect extension
provided in the `/examples` directory.

As explained in the `pattern_importer` example documentation, the extension adds fields to two
alpenhorn tables: `ArchiveAcq` and `ArchiveFile`.  Because of this, we need to run the extension
initialisation before we can create the rest of the data index proper.

To initialise the database for the extension, run the `demo_init` function provided by the
extension:
```
  python -c 'import pattern_importer; pattern_importer.demo_init()'
```

You should see a success message:
```(console)
root@alpen1:/# python -c 'import pattern_importer; pattern_importer.demo_init()'
Plugin init complete complete.
```

### Setting up the data index

Once the pattern importer has been set up, set up the rest of the data index using the alpenhorn
CLI:
```
  alpenhorn db init
```
Remember that all these alpenhorn commands need to be run inside the `alpen1` container that we
started in the last section.  The `db init` command outputs nothing when successful:
```(console)
root@alpen1:/# alpenhorn db init
root@alpen1:/#
```

### Create the first StorageNode

We need to start with a place to put some files.  We'll create the first `StorageNode`, which will
be hosted on `alpen1`.  Before we can do that, though we first need to create a `StorageGroup` to
house the node.  All `StorageNode`s need to be contained in a `StorageGroup`.  Typically each group
contains only a single node, but certain group types support or require multiple nodes (such as the
transport group that we'll create later).

To create the group, which we'll call `demo_storage1`, run:
```
  alpenhorn group create demo_storage1
```
This should create the group:
```
root@alpen1:/# alpenhorn group create demo_storage1
Created storage group "demo_storage1".
```
If instead you get an error: `Error: Group "demo_storage1" already exists.` then likely you're
trying to run this demo using an old instance of the database.  In this case, you can stop the demo
and delete the old database as explained above.

Now that the group is created, we can create a node within it.  We'll also call the node
`demo_storage1`.  (By convention, when a StorageGroup contains only one StorageNode, the node and
group have the same name, though that's not required.)
```
  alpenhorn node create demo_storage1 --group=demo_storage1 --auto-import --root=/data --host=alpen1
```
This command will create a new StorageNode called `demo_storage1` and put it in the
identically-named group.  Auto-import will be turned on, the mount point in the filesystem will be
set to `/data` and we declare it to be available on host `alpen1`:
```(console)
root@alpen1:/# alpenhorn node create demo_storage1 --group=demo_storage1 --auto-import 
              --root=/data --host=alpen1
Created storage node "demo_storage1".
```

That's enough to get us started.  Exit the temporary `alpen1` container:
```
exit
```

Docker should remove the container once you've exited.

## Start the first daemon

Now it's time to start the first daemon.  The alpenhorn container is set-up to run the alpenhorn
daemon by default.  We suggest you do this in a separate sesson from the one where you have the
root prompt on `alpen1` to simplify running `alpenhorn` commands whilst the daemons are running.

```
docker compose up --detach alpen1
```

Note: if you're following along with this demo, the database container should already be running:
```(console)
$ docker compose up --detach alpen1
[+] Running 2/2
 ✔ Container demo-alpdb-1   Running                                                           0.0s
 ✔ Container demo-alpen1-1  Started                                                           0.4s
```
(If the database container is not running, docker compose will start it first).

You should now check the logs for the daemon:
```
docker compose logs alpen1
```
(You can add `--follow` if you wish to have the logs continuously update.)  You'll see the
alpenhorn daemon start up:
```
alpen1-1  | Feb 21 00:38:32 INFO >> [MainThread] Alpenhorn start.
alpen1-1  | Feb 21 00:38:32 INFO >> [MainThread] Loading config file /etc/alpenhorn/alpenhorn.conf
alpen1-1  | Feb 21 00:38:32 INFO >> [MainThread] Loading extension pattern_importer
alpen1-1  | Feb 21 00:38:32 INFO >> [Worker#1] Started.
alpen1-1  | Feb 21 00:38:32 INFO >> [Worker#2] Started.
```
Two worker threads are started because that's what's specified in the `demo/alpenhornd.conf` file.

Almost immediately, the daemon will notice that there are no _active_ ndoes on `alpen1`.  It
will perform this check roughly every ten seconds, which is the update interval time set in
the `demo/alpenhornd.conf` file.
```
alpen1-1  | Feb 21 00:38:32 WARNING >> [MainThread] No active nodes on host (alpen1)!
alpen1-1  | Feb 21 00:38:32 INFO >> [MainThread] Main loop execution was 0.0s.
alpen1-1  | Feb 21 00:38:32 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers
alpen1-1  | Feb 21 00:38:42 WARNING >> [MainThread] No active nodes on host (alpen1)!
alpen1-1  | Feb 21 00:38:42 INFO >> [MainThread] Main loop execution was 0.0s.
alpen1-1  | Feb 21 00:38:42 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers
```

We can fix this by activating the node we created.

Start bash session in the runing `alpen1` container:
```
docker compose exec alpen1 bash -l
```
(Note the use of `exec` here instead of `run` which we used to start the bash session earlier.  The
difference between `exec` and `run` is: `exec` will execute the command in the running `alpen1` container.
Using `run` would have created a separate instance of the `alpen1` container to run the command.

You will be issuing a lot of `alpenhorn` commands over the course of this demo.  We suggest leaving this
root shell open to make it more convenient to issue them.

In the `alpen1` container, at the root prompt, we can now activate the node:
```
alpenhorn node activate demo_storage1
```

```(console)
root@alpen1:/# alpenhorn node activate demo_storage1
Storage node "demo_storage1" activated.
```

Now the daemon will find the active node, but there's still a problem:
```
alpen1-1  | Feb 21 00:40:22 INFO >> [MainThread] Node "demo_storage1" now available.
alpen1-1  | Feb 21 00:40:22 WARNING >> [MainThread] Node file "/data/ALPENHORN_NODE" could not be read.
alpen1-1  | Feb 21 00:40:22 WARNING >> [MainThread] Ignoring node "demo_storage1": not initialised.
alpen1-1  | Feb 21 00:40:22 INFO >> [MainThread] Main loop execution was 0.0s.
alpen1-1  | Feb 21 00:40:22 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers
```

We need to initialise the node so `alpenhorn` can use it.  In this case, we could do this by
manually creating the `/data/ALPENHORN_NODE` file that it can't find.  But, generally, it's easier to
tell the daemon to initialise the node for us:
```
alpenhorn node init demo_storage1
```

```(console)
root@alpen1:/# alpenhorn node init demo_storage1
Requested initialisation of Node "demo_storage1".
```
A node only ever needs to be initialised once, when it is first created, but it's always safe to run this
command: a request to initialise an already-initialised node is simply ignored.

You should see the node being initialised by one of the daemon workers:
```
alpen1-1  | Feb 21 00:40:52 INFO >> [MainThread] Node "demo_storage1" now available.
alpen1-1  | Feb 21 00:40:52 WARNING >> [MainThread] Node file "/data/ALPENHORN_NODE" could not be read.
alpen1-1  | Feb 21 00:40:52 INFO >> [MainThread] Requesting init of node "demo_storage1".
alpen1-1  | Feb 21 00:40:52 INFO >> [MainThread] Main loop execution was 0.0s.
alpen1-1  | Feb 21 00:40:52 INFO >> [Worker#1] Beginning task Init Node "demo_storage1"
alpen1-1  | Feb 21 00:40:52 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
alpen1-1  | Feb 21 00:40:52 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
alpen1-1  | Feb 21 00:40:52 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
alpen1-1  | Feb 21 00:40:52 INFO >> [Worker#1] Node "demo_storage1" initialised.
alpen1-1  | Feb 21 00:40:52 INFO >> [Worker#1] Finished task: Init Node "demo_storage1"
```

After it does that, it will finally be happy with the storage node and start the auto-import monitor.
The start of auto-import triggers a "catch-up" job which searches for unknown, pre-existing files that
need import.  As this is an empty node, though, it won't find anything:
```
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Node "demo_storage1" now available.
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Group "demo_storage1" now available.
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Watching node "demo_storage1" root "/data" for auto import.
alpen1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Beginning task Catch-up on demo_storage1
alpen1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Scanning "." on "demo_storage1" for new files.
alpen1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Scanning ".".
alpen1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Finished task: Catch-up on demo_storage1
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Node demo_storage1: 46.77 GiB available.
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Updating node "demo_storage1".
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Updating group "demo_storage1".
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Main loop execution was 0.0s.
alpen1-1  | Feb 21 00:41:02 INFO >> [MainThread] Tasks: 1 queued, 0 deferred, 0 in-progress on 2 workers
```

It will also run a job to see if there's anything needing clean-up on the node. This "tidy up" job
helps the alpenhorn daemon recover from unexpected crashes.  The job is generally run when a node
first becomes available to the daemon, and then periodically after that.  Again, because this
is a brand-new node, there isn't anything needing tidying:
```
alpen1-1  | Feb 21 00:41:02 INFO >> [Worker#2] Beginning task Tidy up demo_storage1
alpen1-1  | Feb 21 00:41:02 INFO >> [Worker#2] Finished task: Tidy up demo_storage1
alpen1-1  | Feb 21 00:41:12 INFO >> [MainThread] Node demo_storage1: 46.77 GiB available.
alpen1-1  | Feb 21 00:41:12 INFO >> [MainThread] Updating node "demo_storage1".
alpen1-1  | Feb 21 00:41:12 INFO >> [MainThread] Updating group "demo_storage1".
alpen1-1  | Feb 21 00:41:12 INFO >> [MainThread] Main loop execution was 0.0s.
alpen1-1  | Feb 21 00:41:12 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers
```


## Importing files

Let's experiment now with importing files into alpenhorn, using both the auto-import system and manually
importing them.

### What kind of files can be imported?

As mentioned before, alpenhorn itself is agnostic to data files.  All decisions on which files
are imported into the data index are made by the import detect extensions, which can be tailored to
the specific data being managed.  For this demo, the only import detect function we're using is the
example `pattern_importer` extension.  This extension uses a regular expressions to match against the
pathnames of candidate files to determine whether they should be imported or not.

The `demo_init` function that we called earlier to initialise the database for this demo, added one
allowed ArchiveAcq name pattern consisting of a nested directory tree with the date: `YYYY/MM/DD` and
two allowed ArchiveFile name patterns.  The first of these is a file called "meta.txt" in the top
acquisition directory (i.e. `YYYY/MM/DD/meta.txt`), which provides metadata for our notional acquisition,
and then data files with the time of day, sorted further into hourly directories (i.e.
`YYYY/MM/DD/hh/mmss.dat`).

It bears repeating: the _contents_ of these files are not interesting to alpenhorn per se, but a
import detect extension may be implemented which inspects the data of the files being imported, if
desired.

We'll continue this demo by creating files with the above-mentioned naming conventions, without much
concern about the file contents.

### Auto-importing files and lock files

Let's start with auto-importing files.  When auto-import is turned on for a node, like it has been for
our `demo_storage1` node, then files will automatically be discovered by alpenhorn as they are added
to the node filesystem.

Care must be taken when writing files to a node filesystem when auto-import is turned on to prevent
alpenhorn from trying to import a file before it is fully written.  To prevent this from happening,
before creating a file on the node filesystem, we can create a _lock file_.

For a file at the path `AAA/BBB/name.ext`, the corresponding lock file will be called
`AAA/BBB/.name.ext.lock` (i.e. the name of a lock file is the name of the file it's locking plus a
leading `.` and a `.lock` suffix.

Let's create the first file we want to import into alpenhorn, first creating it's lockfile.  This
should be done in the `alpen1` root shell we started earlier:
```
mkdir -p /data/2025/02/21
touch /data/2025/02/21/.meta.txt.lock
echo "This is the first acquistion in the alpenhorn demo" > /data/2025/02/21/meta.txt
```

When creating the file in this last step, you'll see alpenhorn notice the file, but skip it because
it's locked:
```
alpen1-1  | Feb 21 23:04:21 INFO >> [Worker#1] Beginning task Import 2025/02/21/meta.txt on demo_storage1
alpen1-1  | Feb 21 23:04:21 INFO >> [Worker#1] Skipping "2025/02/21/meta.txt": locked.
alpen1-1  | Feb 21 23:04:21 INFO >> [Worker#1] Finished task: Import 2025/02/21/meta.txt on demo_storage1
```
Note: in some cases file creation can cause multiple import requests to be scheduled.  This is
hamless: alpenhorn is prepared to handle multiple simultaneous attempts to import the same file and will
only ever import a file once.

Once the file has been created, the lock file can be deleted, to trigger import of the file:
```
rm -f /data/2025/02/21/.meta.txt.lock
```

This will trigger alpenhorn to finally actually import the file:
```
alpen1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Beginning task Import 2025/02/21/meta.txt on demo_storage1
alpen1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Acquisition "2025/02/21" added to DB.
alpen1-1  | Feb 21 23:07:07 INFO >> [Worker#1] File "2025/02/21/meta.txt" added to DB.
alpen1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Imported file copy "2025/02/21/meta.txt" on node "demo_storage1".
alpen1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Finished task: Import 2025/02/21/meta.txt on demo_storage1
```

Note here that the the three lines in the middle of the daemon output above indicate that the daemon
has created three new records in the database:
* an `ArchiveAcq` record for the new acquisition, with name `2025/02/21`
* an `ArchiveFile` record for the new file, with name `21/meta.txt` in the new acqusition
* an `ArchiveFileCopy` record recording that a copy of the newly-created `ArchiveFile` exists on `demo_storage1`


You can use the alpenhorn CLI to see that this file is now present on the `demo_storage1` node:
```
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             1          51 B         -
root@alpen1:/# alpenhorn file list --node=demo_storage1 --details
File                 Size    MD5 Hash                          Registration Time             State    Size on Node
-------------------  ------  --------------------------------  ----------------------------  -------  --------------
2025/02/21/meta.txt  51 B    c2607e3dbaf6a1e2467b82c6a79f6b46  Fri Feb 21 23:07:08 2025 UTC  Healthy  4.000 kiB
```

### Auto-importing files and temporary names

Another option for writing files to a node filesystem when auto-import is turned on, may be to
write them to a path which the import detect extensions won't recognise, although whether this
is possible will depend on the particular import detect extensions being used.  For this demo,
we can use any name which doesn't match the patterns which the `pattern_importer` will accept.

As an example, let's create a `.dat` file with a temporary name by appending, say, `.temp` to
the name of the file we want to create:
```
mkdir /data/2025/02/21/23
echo "0 1 2 3 4 5" > /data/2025/02/21/23/1324.dat.temp
```

This file creation will be noticed by alpenhorn, but no import will occur, because the
`pattern_exporter` won't accept the name as valid:
```
alpen1-1  | Feb 21 23:51:59 INFO >> [Worker#1] Beginning task Import 2025/02/21/23/1324.dat.temp on demo_storage1
alpen1-1  | Feb 21 23:51:59 INFO >> [Worker#1] Not importing non-acquisition path: 2025/02/21/23/1324.dat.temp
alpen1-1  | Feb 21 23:51:59 INFO >> [Worker#1] Finished task: Import 2025/02/21/23/1324.dat.temp on demo_storage1
```

After file is fully written, it can be moved to the correct name.  On most filesystems, this is an
atomic operation:
```
mv /data/2025/02/21/23/1324.dat.temp /data/2025/02/21/23/1324.dat
```

This will trigger import of the file:
```
alpen1-1  | Feb 21 23:52:20 INFO >> [Worker#2] Beginning task Import 2025/02/21/23/1324.dat on demo_storage1
alpen1-1  | Feb 21 23:52:20 INFO >> [Worker#2] File "2025/02/21/23/1324.dat" added to DB.
alpen1-1  | Feb 21 23:52:20 INFO >> [Worker#2] Imported file copy "2025/02/21/23/1324.dat" on node "demo_storage1".
alpen1-1  | Feb 21 23:52:20 INFO >> [Worker#2] Finished task: Import 2025/02/21/23/1324.dat on demo_storage1
```

Unlike when we imported the first file, now only two new records are created in the database. because
the acquistion record already existsed:
* an `ArchiveFile` for the new file
* an `ArchiveFileCopy` for the copy of the new file on `demo_storage1`

Now there are two files on the node:
```
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             2          63 B         -
root@alpen1:/# alpenhorn file list --node=demo_storage1 --details
File                    Size    MD5 Hash                          Registration Time             State    Size on Node
----------------------  ------  --------------------------------  ----------------------------  -------  --------------
2025/02/21/23/1324.dat  12 B    4c79018e00ddef11af0b9cfc14dd3261  Fri Feb 21 23:52:21 2025 UTC  Healthy  4.000 kiB
2025/02/21/meta.txt     51 B    c2607e3dbaf6a1e2467b82c6a79f6b46  Fri Feb 21 23:07:08 2025 UTC  Healthy  4.000 kiB
```

### Manually importing files

Let's now turn to the case where we _don't_ have auto-import turned on for a node.  In this case
there's no difficulty writing to the node, since filesystem events won't trigger automatic attempts
to import files.

First, turn off auto-import on the node:
```
alpenhorn node modify demo_storage1 --no-auto-import
```

If you want, you can verify that auto-import has been turned off for the node by checking its properties:
```
root@alpen1:/# alpenhorn node modify demo_storage1 --no-auto-import
Node updated.
root@alpen1:/# alpenhorn node show demo_storage1
   Storage Node: demo_storage1
  Storage Group: demo_storage1
         Active: Yes
           Type: -
          Notes: 
      I/O Class: Default

    Daemon Host: alpen1
 Log-in Address: 
Log-in Username: 

    Auto-Import: Off
    Auto-Verify: Off
      Max Total: -
      Available: 46.47 GiB
  Min Available: -
   Last Checked: Sat Feb 22 00:03:36 2025 UTC

I/O Config:

  none
```

With that done, let's create some more data files:
```
echo "0 1 2 3 4 5" > /data/2025/02/21/23/1330.dat
echo "3 4 5 6 7 8" > /data/2025/02/21/23/1342.dat
echo "9 10 11 12 13" > /data/2025/02/21/23/1349.dat
```

None of these files have been added to the database.  We can use the alpenhorn CLI to see this: as
far as alpenhorn is concerned, there are still only two files on the node.
```
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             2          63 B         -
```

But, now that we've finished writing these files, we can tell alpenhorn to import them.  This can
be done for an individual file:
```
alpenhorn file import --register-new 2025/02/21/23/1330.dat demo_storage1
```

The `--register-new` flag tells alpenhorn that it is allowed to create a new `ArchiveFile` (and,
were it necessary, an `ArchiveAcq` record, too) for newly discovered files.  Without this flag,
alpenhorn will only import files which are already represented by an existing `ArchvieFile`.  This
second mode is more appropriate in cases where a node should not be receiving new files.

The CLI will create an import request for this file:
```
root@alpen1:/# alpenhorn file import --register-new 2025/02/21/23/1330.dat demo_storage1
Added new import request.
```

The import request should be shortly handled by the daemon:
```
alpen1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Beginning task Import 2025/02/21/23/1330.dat on demo_storage1
alpen1-1  | Feb 22 00:09:36 INFO >> [Worker#1] File "2025/02/21/23/1330.dat" added to DB.
alpen1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Imported file copy "2025/02/21/23/1330.dat" on node "demo_storage1".
alpen1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Completed import request #2.
alpen1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Finished task: Import 2025/02/21/23/1330.dat on demo_storage1
```

It's also possible to tell alpenhorn to scan an entire directory for new files.
```
alpenhorn node scan demo_storage1 --register-new 2025/02/21
```

Which will add another import request:
```
root@alpen1:/# alpenhorn node scan demo_storage1 --register-new 2025/02/21
Added request for scan of "2025/02/21" on Node "demo_storage1".
```

Now alpenhorn will scan the requested path and find the other two files we just created:
```
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Beginning task Scan "2025/02/21" on demo_storage1
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Scanning "2025/02/21" on "demo_storage1" for new files.
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Scanning "2025/02/21".
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Scanning "2025/02/21/23".
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#1] Beginning task Import 2025/02/21/23/1349.dat on demo_storage1
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Completed import request #4.
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Finished task: Scan "2025/02/21" on demo_storage1
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Beginning task Import 2025/02/21/23/1342.dat on demo_storage1
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#1] File "2025/02/21/23/1349.dat" added to DB.
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#1] Imported file copy "2025/02/21/23/1349.dat" on node "demo_storage1".
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] File "2025/02/21/23/1342.dat" added to DB.
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Imported file copy "2025/02/21/23/1342.dat" on node "demo_storage1".
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#1] Finished task: Import 2025/02/21/23/1349.dat on demo_storage1
alpen1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Finished task: Import 2025/02/21/23/1342.dat on demo_storage1
```

Now there are five files on the storage node:
```
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             5         101 B         -
```

## Syncing files between nodes

Let's now move on to syncing, or transferring, files between different hosts.

### Starting up the second and third nodes

Before being able to transfer files, we need to create somewhere to transfer them to.
We'll start by creating the second storage nodes, on the second host:

```
alpenhorn node create demo_storage2 --create-group --root=/data --host=alpen2
```

Note here that the `--create-group` option to `node create` tells alpenhorn to also create a
`StorageGroup` for the new node with the same name (i.e. the same thing we did manually
for `demo_storage1` above):
```
root@alpen1:/# alpenhorn node create demo_storage2 --create-group --root=/data --host=alpen2
Created storage group "demo_storage2".
Created storage node "demo_storage2".
```

Let's also make sure this node gets initialised, though this won't happen immediately,
since we haven't activated the Storage Node, nor are we running the second daemon yet.
```
alpenhorn node init demo_storage2
```
Requests created by the alpenhorn CLI, be they initialisation requests, import requests,
or transfer requests, do not require the target node to be active, nor do they require an
alpenhorn daemon to be managing them.  Requests made on inactive nodes will remain pending
in the database until they can be handled by an alpenhorn daemon instance.

You can see pending requests, including this import request, using the alpenhorn CLI:
```
root@alpen1:/# alpenhorn node show demo_storage2 --all
   Storage Node: demo_storage2
  Storage Group: demo_storage2
         Active: No
           Type: -
          Notes:
      I/O Class: Default

    Daemon Host: alpen2
 Log-in Address:
Log-in Username:

    Auto-Import: Off
    Auto-Verify: Off
      Max Total: -
      Available: -
  Min Available: -
   Last Checked: -

I/O Config:

  none

Stats:

    Total Files: 0
     Total Size: -
          Usage: -%

Pending import requests:

Path         Scan    Register New    Request Time
-----------  ------  --------------  -------------------
[Node Init]  -       -               2025-02-26 22:54:14

Pending outbound transfers:

Dest. Group    Request Count    Total Size
-------------  ---------------  ------------

Auto-actions:

  none
```
(Node init requests are handled, under the hood, as a special kind of import request.)

This nodes is initially empty:
```
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             5         101 B         -
demo_storage2             0             -         -
```

Before starting transfers we have to record log-in details for the hosts containing the nodes.
alpenhorn uses SSH to log in to remote nodes when performing transfers, meaning we need to
specify a username and login-in address for the node.  For `demo_storage1`, which is already
active we can do this by modifying the node record:
```
alpenhorn node modify demo_storage1 --username root --address alpen1
```

For the second node, we can do it when we activate it.  (We could have also specified
these values when we created the node.):
```
alpenhorn node activate demo_storage2 --username root --address alpen2
```

Note: it's very important to distinguish the name used for a node's _host_ (where the
daemon managing the node is running) and the node's _address_ (the name or IP address
used by remote daemons to access the node via SSH).  Often these two fields have the same
value, but there's no requirement that they do.

Let's start up the second alpenhorn container to get the second node running:
```
docker compose up --detach alpen2
```

You can monitor this nodes in the same way you did with alpen1:
```
docker compose logs alpen2
```
but it may be easier to monitor all nodes at once:
```
docker compose logs --follow
```

For now, the new node should initialise itself, and then idle: there are no pending requests:
```
alpen2-1  | Feb 26 23:05:02 INFO >> [MainThread] Node "demo_storage2" now available.
alpen2-1  | Feb 26 23:05:02 WARNING >> [MainThread] Node file "/data/ALPENHORN_NODE" could not be read.
alpen2-1  | Feb 26 23:05:02 INFO >> [MainThread] Requesting init of node "demo_storage2".
alpen2-1  | Feb 26 23:05:02 INFO >> [MainThread] Main loop execution was 0.0s.
alpen2-1  | Feb 26 23:05:02 INFO >> [MainThread] Tasks: 1 queued, 0 deferred, 0 in-progress on 2 workers
alpen2-1  | Feb 26 23:05:02 INFO >> [Worker#1] Beginning task Init Node "demo_storage2"
alpen2-1  | Feb 26 23:05:02 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
alpen2-1  | Feb 26 23:05:02 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
alpen2-1  | Feb 26 23:05:02 INFO >> [Worker#1] Node "demo_storage2" initialised.
alpen2-1  | Feb 26 23:05:02 INFO >> [Worker#1] Finished task: Init Node "demo_storage2"
alpen2-1  | Feb 26 23:05:12 INFO >> [MainThread] Node "demo_storage2" now available.
alpen2-1  | Feb 26 23:05:12 INFO >> [MainThread] Group "demo_storage2" now available.
alpen2-1  | Feb 26 23:05:12 INFO >> [MainThread] Node demo_storage2: 45.51 GiB available.
alpen2-1  | Feb 26 23:05:12 INFO >> [MainThread] Updating node "demo_storage2".
alpen2-1  | Feb 26 23:05:12 INFO >> [MainThread] Updating group "demo_storage2".
alpen2-1  | Feb 26 23:05:12 INFO >> [MainThread] Main loop execution was 0.0s.
alpen2-1  | Feb 26 23:05:12 INFO >> [MainThread] Tasks: 1 queued, 0 deferred, 0 in-progress on 2 workers
alpen2-1  | Feb 26 23:05:12 INFO >> [Worker#1] Beginning task Tidy up demo_storage2
alpen2-1  | Feb 26 23:05:12 INFO >> [Worker#1] Finished task: Tidy up demo_storage2
```
   
### Transferring a file

The alpenhorn daemon has the ability to transfer files between Storage Nodes.  To trigger
file movement, we need to issue sync or transfer requests.  Transfer requests _always_
request movement of a file from a Storage Node into a Storage Group.  Because all the
groups we have for now have a single node in them, this distinction isn't terribly important,
but we'll revisit this later, when we experiment with multi-node groups.

We can transfer any existing file explicitly by issuing a transfer request for it:
```
alpenhorn file sync --from demo_storage1 --to demo_storage2 2025/02/21/meta.txt
```

This will submit a new transfer request:
```(console)
root@alpen1:/# alpenhorn file sync --from demo_storage1 --to demo_storage2 2025/02/21/meta.txt
Request submitted.
```
Transfers are always handled on the receiving side.  After, perhaps, a short while, the
daemon on `alpen2` will notice this request.  First, it will look at the local filesystem to
see if the requested file already exists.  If it did, there would be no need for a transfer:
```
alpen2-1  | Feb 26 23:18:52 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/meta.txt in demo_storage2
alpen2-1  | Feb 26 23:18:52 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/meta.txt in demo_storage2
```

But, in this case, the search will fail to find an existing copy of the file, so then a file
transfer will be started:
```
alpen2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Beginning task AFCR#1: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Creating directory "/data/2025/02/21".
alpen2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Pulling remote file 2025/02/21/meta.txt using rsync
alpen2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Pull of 2025/02/21/meta.txt complete. Transferred 51 B in 0.4s [139 B/s]
alpen2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Finished task: AFCR#1: demo_storage1 -> demo_storage2
```

The default tool for remote transfers is `rsync`, but alpenhorn will also try to use `bbcp`, a GridFTP
implementation, which allows for higher-rate transfers, if it is available on to the daemon.

Now there is one file on `demo_storage2`:
```
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             5         101 B         -
demo_storage2             1          51 B         -
```

You can check the filesystem on `alpen2` to see that this file now exists on that node:
```(console)
$ docker compose exec alpen2 find /data
/data
/data/ALPENHORN_NODE
/data/2025
/data/2025/02
/data/2025/02/21
/data/2025/02/21/meta.txt
```

### Bulk transfers

Rather than the tedious operation of requesting individual files for transfer, it is more typical
to request _all_ files present on a source node and absent from a destination be transferred:
```
alpenhorn node sync demo_storage1 demo_storage2 --show-files
```
This will cause the alpenhorn CLI to create transfer requests for all files not present on
`demo_storage2` which are present on `demo_storage1`.

This command will require confirmation:
```
root@alpen1:/# alpenhorn node sync demo_storage1 demo_storage2 --show-files
Would sync 4 files (50 B) from Node "demo_storage1" to Group "demo_storage2":

2025/02/21/23/1324.dat
2025/02/21/23/1330.dat
2025/02/21/23/1342.dat
2025/02/21/23/1349.dat

Continue? [y/N]: y

Syncing 4 files (50 B) from Node "demo_storage1" to Group "demo_storage2".

Added 4 new copy requests.
```
Although there are five files on the node, only four of them will be transferred, because
the first file we transferred is already on `demo_storage2`.

The daemon on alpen2 will churn through these requests:
```
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/23/1330.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/23/1330.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Beginning task AFCR#2: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/23/1324.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Creating directory "/data/2025/02/21/23".
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/23/1324.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task AFCR#3: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pulling remote file 2025/02/21/23/1324.dat using rsync
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pulling remote file 2025/02/21/23/1330.dat using rsync
alpen2-1  | Feb 26 23:34:32 INFO >> [MainThread] Main loop execution was 0.1s.
alpen2-1  | Feb 26 23:34:32 INFO >> [MainThread] Tasks: 2 queued, 0 deferred, 2 in-progress on 2 workers
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pull of 2025/02/21/23/1324.dat complete. Transferred 12 B in 0.3s [36 B/s]
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pull of 2025/02/21/23/1330.dat complete. Transferred 12 B in 0.3s [36 B/s]
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: AFCR#3: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Finished task: AFCR#2: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/23/1349.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Beginning task Pre-pull search for 2025/02/21/23/1342.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/23/1349.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task AFCR#4: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Finished task: Pre-pull search for 2025/02/21/23/1342.dat in demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Beginning task AFCR#5: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pulling remote file 2025/02/21/23/1349.dat using rsync
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pulling remote file 2025/02/21/23/1342.dat using rsync
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pull of 2025/02/21/23/1342.dat complete. Transferred 12 B in 0.4s [32 B/s]
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pull of 2025/02/21/23/1349.dat complete. Transferred 14 B in 0.4s [37 B/s]
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Finished task: AFCR#5: demo_storage1 -> demo_storage2
alpen2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: AFCR#4: demo_storage1 -> demo_storage2
```

And eventually all files will be transfered to `alpen2`:
```
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             5         101 B         -
demo_storage2             5         101 B         -
```

Note: If you were to try the identical sync request a second time, after alpen2 has finished all the
transfers, alpenhorn will decide that nothing needs transferring:
```(console)
root@alpen1:/# alpenhorn node sync demo_storage1 demo_storage2 --show-files
No files to sync.
```

One last note on the `node sync` command: if you prefer thinking about the destination side
of transfers, you can use `group sync` to peform the same task.

The command
```
alpenhorn node sync demo_storage1 demo_storage2 --show-files
```
is equivalent to
```
alpenhorn group sync demo_storage2 demo_storage1 --show-files
```
though note that with `node sync` the arguments are source node and then destination group
but with `group sync` these are reversed: the first argument is the destination group and the
second argument the source node.

## Dealing with corruption

More than just helping you copy files around, alpenhorn can monitor your files for corruption.

### MD5 Digest Hashes

Although, as mentioned earlier, alpenhorn doesn't really know what's in the files its managing,
whenever it registers a new file, it computes the MD5 digest hash for the file.  This means that,
if a file is changed after registration, alpenhorn can detect this change by re-computing the
MD5 hash and comparing it to the hash value it recorded when first registering the file.

You can see the stored hash value for a file using the alpenhorn CLI:
```(console)
root@alpen1:/# alpenhorn file show 2025/02/21/23/1324.dat                  
       Name: 23/1324.dat
Acquisition: 2025/02/21
       Path: 2025/02/21/23/1324.dat

       Size: 12 B
   MD5 Hash: 4c79018e00ddef11af0b9cfc14dd3261
 Registered: Thu Mar  6 22:54:37 2025 UTC
```

If we were to manually compute the MD5 digest for this file we would get the same result:
```(console)
root@alpen1:/# md5sum /data/2025/02/21/23/1324.dat
4c79018e00ddef11af0b9cfc14dd3261  /data/2025/02/21/23/1324.dat
```

Let's corrupt a file by changing its contents:

```
echo "bad data" > /data/2025/02/21/23/1324.dat
```

Now if we manually compute the MD5 hash, we can see that's it's different than
what alpenhorn has recorded:

```(console)
root@alpen1:/# md5sum /data/2025/02/21/23/1324.dat
3412f7b66a30b90ae3d3085c96615f00  /data/2025/02/21/23/1324.dat
```

However, alpenhorn has noticed this:

```(console)
root@alpen1:/# alpenhorn node stats --extra-stats
Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
-------------  ------------  ------------  --------  ---------------  ---------------  ---------------
demo_storage1             5         101 B         -                -                -                -
demo_storage2             5         101 B         -                -                -                -
```
It still lists no corrupt files on `demo_storage1`.  This is because alpenhorn
doesn't normally automatically detect corruption to files it is managing.  You
can turn on "auto-verify" on a node, but that can be I/O expensive, and should be
used with caution.

In some cases, file corruption can be detected by alenhorn when copying an unexpectedly corrupt
file from one node to another.  For now, we can manually request a verification of the file.
We'll do this by requesting verifciation for the entire acqusition, even though we've only
corrupted one of the files.

To request verifcation of all files in the acqusition, run:
```
alpenhorn node verify --all --acq=2025/02/21 demo_storage1
```

You will have to confirm this request:
```(console)
root@alpen1:/# alpenhorn node verify --all --acq=2025/02/21 demo_storage1
Would request verification of 5 files (101 B).

Continue? [y/N]: y

Requesting verification of 5 files (101 B).
Updated 5 files.
```

The daemon on `alpen1` will respond to this command by re-verifying all files in that acqusition:
```
alpen1-1  | Mar 07 01:48:25 INFO >> [MainThread] Checking copy "2025/02/21/23/1349.dat" on node demo_storage1.
alpen1-1  | Mar 07 01:48:25 INFO >> [MainThread] Checking copy "2025/02/21/23/1342.dat" on node demo_storage1.
alpen1-1  | Mar 07 01:48:25 ERROR >> [Worker#2] File 2025/02/21/23/1324.dat on node demo_storage1 is corrupt! Size: 9; expected: 12
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Updating file copy #2 for file 2025/02/21/23/1324.dat on node demo_storage1.
alpen1-1  | Mar 07 01:48:25 INFO >> [MainThread] Updating group "demo_storage1".
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Finished task: Check file 2025/02/21/23/1324.dat on demo_storage1
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Beginning task Check file 2025/02/21/23/1330.dat on demo_storage1
alpen1-1  | Mar 07 01:48:25 INFO >> [MainThread] Main loop execution was 0.0s.
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#1] File 2025/02/21/meta.txt on node demo_storage1 is A-OK!
alpen1-1  | Mar 07 01:48:25 INFO >> [MainThread] Tasks: 2 queued, 0 deferred, 2 in-progress on 2 workers
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Updating file copy #1 for file 2025/02/21/meta.txt on node demo_storage1.
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Finished task: Check file 2025/02/21/meta.txt on demo_storage1
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Beginning task Check file 2025/02/21/23/1349.dat on demo_storage1
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] File 2025/02/21/23/1330.dat on node demo_storage1 is A-OK!
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Updating file copy #3 for file 2025/02/21/23/1330.dat on node demo_storage1.
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Finished task: Check file 2025/02/21/23/1330.dat on demo_storage1
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Beginning task Check file 2025/02/21/23/1342.dat on demo_storage1
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#1] File 2025/02/21/23/1349.dat on node demo_storage1 is A-OK!
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Updating file copy #4 for file 2025/02/21/23/1349.dat on node demo_storage1.
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] File 2025/02/21/23/1342.dat on node demo_storage1 is A-OK!
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Updating file copy #5 for file 2025/02/21/23/1342.dat on node demo_storage1.
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Finished task: Check file 2025/02/21/23/1349.dat on demo_storage1
alpen1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Finished task: Check file 2025/02/21/23/1342.dat on demo_storage1
```
As you can see, it has discovered our corruption of `2025/02/21/23/1324.dat`.

Now if we check the node stats, we can see one corrupt file on this node.
```
root@alpen1:/# alpenhorn node stats --extra-stats
Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
-------------  ------------  ------------  --------  ---------------  ---------------  ---------------
demo_storage1             4          89 B         -                1                -                -
demo_storage2             5         101 B         -                -                -                -
root@alpen1:/# alpenhorn file state 2025/02/21/23/1324.dat demo_storage1
Corrupt Ready
```

## Recovering corrupt files

The standard way to recover a corrupt file copy is to re-transfer a known-good copy of the
file over the corrupt version.  We can do this by syncing the files back from `alpen2`:

```
alpenhorn node sync demo_storage2 demo_storage1
```

It will tell you there is one file to transfer (the corrupt file) and ask for confirmation:
```(console)
root@alpen1:/# alpenhorn node sync demo_storage2 demo_storage1
Would sync 1 file (12 B) from Node "demo_storage2" to Group "demo_storage1".

Continue? [y/N]: y

Syncing 1 file (12 B) from Node "demo_storage2" to Group "demo_storage1".

Added 1 new copy request.
```

Wait for the daemon on `alpen1` to pull the file from `alpen2`:
```
alpen1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Beginning task AFCR#6: demo_storage2 -> demo_storage1
alpen1-1  | Mar 07 01:52:15 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
alpen1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Pulling remote file 2025/02/21/23/1324.dat using rsync
alpen1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Pull of 2025/02/21/23/1324.dat complete. Transferred 12 B in 0.4s [32 B/s]
alpen1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Finished task: AFCR#6: demo_storage2 -> demo_storage1
```

After transferring the file back, now alpenhorn now considers the file healthy again:
```(console)
root@alpen1:/# alpenhorn node stats --extra-stats
Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
-------------  ------------  ------------  --------  ---------------  ---------------  ---------------
demo_storage1             5         101 B         -                -                -                -
demo_storage2             5         101 B         -                -                -                -
```

## Deleting files

Typically you'll want to delete files off your acqusition nodes once they've been transferred off-site.
File deletion can be accomplished with the `clean` command.

Since we've copied some files from `alpen1` to `alpen2`, let's try deleting one of the files from `alpen1`:
```
alpenhorn file clean --now --node=demo_storage1 2025/02/21/meta.txt
```

The CLI should release the file immediately:
```(console)
root@alpen1:/# alpenhorn file clean --now --node=demo_storage1 2025/02/21/meta.txt
Released "2025/02/21/meta.txt" for immediate removal on Node "demo_storage1".
```

However, if you look at the daemon log on `alpen1`, you'll see that it's refused to delete the file:
```
alpen1-1  | Mar 07 02:21:25 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
alpen1-1  | Mar 07 02:21:25 WARNING >> [Worker#1] Too few archive copies (0) to delete 2025/02/21/meta.txt on demo_storage1.
alpen1-1  | Mar 07 02:21:25 INFO >> [Worker#1] Finished task: Delete copies [1] from demo_storage1
```

To prevent data loss, alpenhorn will only delete file copies if at least two other copies
of the file exist on archive nodes.  Currently we have no archive nodes, so we can't delete files.

Let's fix that.  While we do, the `alpen1` daemon will keep checking whether it can delete that file.

### Archive nodes

An archive node is any storage node with the "archive" storage type.  Let's change `demo_storage2` into
an archive node.  We do that by modifying it's metadata:
```
alpenhorn node modify --archive demo_storage2
```

After running this command, you can look at the node metadata to see that it now has the "archive" storage type:
```(console)
root@alpen1:/# alpenhorn node modify --archive demo_storage2
Node updated.
root@alpen1:/# alpenhorn node show demo_storage2
   Storage Node: demo_storage2
  Storage Group: demo_storage2
         Active: Yes
           Type: Archive
          Notes: 
      I/O Class: Default

    Daemon Host: alpen2
 Log-in Address: alpen2
Log-in Username: root

    Auto-Import: Off
    Auto-Verify: Off
      Max Total: -
      Available: 45.38 GiB
  Min Available: -
   Last Checked: Fri Mar  7 02:27:47 2025 UTC

I/O Config:

  none
```

Now if we look at the `alpen1` daemon log, the file it's trying to delete is now found on
one archive node (out of the two needed):
```
alpen1-1  | Mar 07 02:28:55 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
alpen1-1  | Mar 07 02:28:55 WARNING >> [Worker#1] Too few archive copies (1) to delete 2025/02/21/meta.txt on demo_storage1.
alpen1-1  | Mar 07 02:28:55 INFO >> [Worker#1] Finished task: Delete copies [1] from demo_storage1
```

We'll need another archive node with this file on it if we want the deletion to happen.  So, let's
create the final storage host, `alpen3`.

First let's set up the storage node in the database.  We can make this one an archive node when we
create it:
```
alpenhorn node create demo_storage3 --create-group --archive --root=/data --host=alpen3 \
                                    --username root --address alpen3 --init --activate
```
The `--init` and `--activate` flags save us from having to run those commands on the new node later.

Now let's start the third docker container and take a look at its logs:
```
docker compose up --detach alpen3
docker compose logs --follow alpen3
```

Sync everything on `demo_storage1` to `demo_storage3`:
```
alpenhorn node sync --force demo_storage1 demo_storage3
```
Using `--force` here skips the confirmation step.

As soon as the file is transferred to `demo_storage3`, the daemon on `alpen1` will happily
delete the file:

```
alpen1-1  | Mar 07 02:38:45 INFO >> [Worker#1] Beginning task Delete copies [1] from demo_storage1
alpen1-1  | Mar 07 02:38:45 INFO >> [Worker#1] Removed file copy 2025/02/21/meta.txt on demo_storage1
alpen1-1  | Mar 07 02:38:45 INFO >> [Worker#1] Finished task: Delete copies [1] from demo_storage1
alpen1-1  | Mar 07 02:38:45 INFO >> [MainThread] Main loop execution was 0.1s.
alpen1-1  | Mar 07 02:38:45 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers
```

Now there are only four files on `demo_storage1`:
```(console)
root@alpen1:/# alpenhorn node stats --extra-stats
Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
-------------  ------------  ------------  --------  ---------------  ---------------  ---------------
demo_storage1             4          50 B         -                -                -                -
demo_storage2             5         101 B         -                -                -                -
demo_storage3             5         101 B         -                -                -                -
```

Rather than cleaning individual files, we can do bulk operations.  To tell alpenhorn to delete
everything from `demo_storage1` that already exists on `demo_storage3`:
```
alpenhorn node clean demo_storage1 --now --target demo_storage3
```

It will find four files to clean, which you'll have to confirm:
```
root@alpen1:/# alpenhorn node clean demo_storage1 --now --target demo_storage3
Would release 4 files (50 B).

Continue? [y/N]: y

Releasing 4 files (50 B).
Updated 4 files.
```

The files will be removed from `demo_storage1` by the daemon:
```
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Beginning task Delete copies [2, 3, 4, 5] from demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1324.dat on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1330.dat on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1349.dat on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1342.dat on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025/02/21/23 on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025/02/21 on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025/02 on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025 on demo_storage1
alpen1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Finished task: Delete copies [2, 3, 4, 5] from demo_storage1
```
Note that the daemon will also delete directories on the node which end up empty after file deletion
to keep the storage node directory tree tidy.

Now `demo_storage1` is empty:
```(console)
root@alpen1:/# alpenhorn node stats --extra-stats
Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
-------------  ------------  ------------  --------  ---------------  ---------------  ---------------
demo_storage1             0             -         -                -                -                -
demo_storage2             5         101 B         -                -                -                -
demo_storage3             5         101 B         -                -                -                -
root@alpen1:/# find /data
/data
/data/ALPENHORN_NODE
```

## Transport disks and the Sneakernet

Alpenhorn has been designed to work with instruments in remote locations where network transport
of data may be difficult or impossible to accomplish.  To help with this situation, alpenhorn
can be used to manage transfer of data via physically moving storage media from site to site.
(This is known as the Sneakernet).

Alpenhorn can be configured to copy data onto a set of physical media at one location where
data are produced and then, later, copy data off those media once they have been transported
to a data ingest site.

To demonstrate this, we'll use a transport disk to simulate transferring data back from
`demo_storage3` to `demo_storage1`, as if these two nodes were unable to communicate directly
over the network.

### The Transport Group and Transport Nodes

In alpenhorn, each individual physical device holding data to transfer is represent by its own
StorageNode which has the "transport" storage type.  All the transport nodes are collected into
a StorageGroup which has the "Transport" I/O class.

Our first job, then, is to create a transport group:
```
alpenhorn group create --class=Transport transport_group
```
This has I/O class "Transport" (the capital "T" is important).  Typically you only ever need one
transport group, and you put all your transport nodes in the single group.  Normal logistics of
the Sneakernet mean that typically different member nodes of this group will be located at different
sites and/or be in-transit at any given time, and the locations of the nodes will change over time.
Alpenhorn never requires, nor expects, multiple nodes in the transport group to be accessible to a
single daemon.

Now that we have the transport group, we can create storage nodes to put in it.  As mentioned
above, each node is a single physical device (disk, tape, etc.) which is transferred through the
Snearkernet.  Multiple nodes in the group can be available at a particular site, but we'll just
create a single node for the purpose of this demo.

When we create the new node, we'll tell alpenhorn that it's initally available on `alpen3`:
```
alpenhorn node create transport1 --transport --group transport_group --host=alpen3 \
                                 --root=/mnt/transport --init --activate
```
Note the use of the `--transport` flag to set the node's storage type to "transport".  The
"Transport" Group I/O class allows StorageNodes of any class to be added to the group, but
requires all such nodes to have the "transport" storage type.

The filesystem has already been made available in the `alpen3` container, so wait for the
daemon on `alpen3` to initialise the node:
```
alpen3-1  | Mar 07 09:21:03 INFO >> [MainThread] Node "transport1" now available.
alpen3-1  | Mar 07 09:21:03 WARNING >> [MainThread] Node file "/mnt/transport/ALPENHORN_NODE" could not be read.
alpen3-1  | Mar 07 09:21:03 INFO >> [MainThread] Requesting init of node "transport1".
alpen3-1  | Mar 07 09:21:03 INFO >> [Worker#1] Beginning task Init Node "transport1"
alpen3-1  | Mar 07 09:21:03 WARNING >> [Worker#1] Node file "/mnt/transport/ALPENHORN_NODE" could not be read.
alpen3-1  | Mar 07 09:21:03 WARNING >> [Worker#1] Node file "/mnt/transport/ALPENHORN_NODE" could not be read.
alpen3-1  | Mar 07 09:21:03 INFO >> [Worker#1] Node "transport1" initialised.
```

### Copying Data to the Transport Group

Remember that, when copy files, data always flows from a node to a group.  To get
data into the transport node, we need to transfer data into the transport group.  Logic
within the Transport I/O class then determines which of the available transport nodes the
transferred files will be written to.

Briefly, the Transport logic works like this:
* only local transfers are allowed into the group (i.e. transferring into the transport
  group will only ever copy data onto transport nodes at the same location as the source
  node).
* the transport group will try to fill up one transport node before copying data to another

In our case we only have a single transport node, so it's easy to figure out which node the
data will end up on.

Let's transfer data out of `demo_storage3` into the transport group with the intent of
transferring data, ultimately, to `demo_storage1`:

```
alpenhorn node sync demo_storage3 transport_group --target=demo_storage1
```
The `--target` option indicates to alpenhorn the ultimate destination for the data
we're syncing to transport.  This prevents alpenhorn from trying to transfer data already
present on `demo_storage1` (though in our case, that's nothing).

This should sync all five files we have:
```
root@alpen1:/# alpenhorn node sync demo_storage3 transport_group --target=demo_storage1
Would sync 5 files (101 B) from Node "demo_storage3" to Group "transport_group".

Continue? [y/N]: y

Syncing 5 files (101 B) from Node "demo_storage3" to Group "transport_group".

Added 5 new copy requests.
```
It may also be good to point out here that even though both `demo_storage3` and the
transport node are on `alpen3`, and all the resulting transfers are local, we can
run this command on `alpen1`.  Running commands with the CLI never need to occur
where the storage nodes referenced are.  Anywhere that can access the alpenhorn
databse can be used to run any alpenhorn command.

After waiting for the daemon to process these requests, a look at the trasnport group
should show us that they've all ended up on the transport node:
```
root@alpen1:/# alpenhorn group show --node-stats transport_group
Storage Group: transport_group
        Notes: 
    I/O Class: Transport

I/O Config:

  none

Nodes:

Name          File Count  Total Size    % Full
----------  ------------  ------------  --------
transport1             5  101 B         -
```

### Transporting the transport node

Now let's simulate what would happen if we wanted to move this transport node
from `alpen3` to `alpen1` over the Snearkernet.  (Normally, to increase throughput
of the Snearkernet, we would wait for the node to fill up.)

The first step is to deactivate the alpenhorn node:
```
alpenhorn node deactivate transport1
```

The daemon on `alpen3` will notice this, and stop updating the transport node:
```
alpen3-1  | Mar 09 04:10:07 INFO >> [MainThread] Node "transport1" no longer available.
alpen3-1  | Mar 09 04:10:07 INFO >> [MainThread] Group "transport_group" no longer available.
```

If this were a real transport device, the next steps would be to:
* unmount the filesystem
* eject the media
* remove the device from the machine

Next, the trasnport device would need to travel (via Sneakernet) from the site containing
`alpen3` to the site containing `alpen1` where we would do the reverse procedure, installing
the device in the `alpen1` machine and mounting the device's filesystem.

For the purposes of this demo, however, we don't have to do any of that: the docker volume we're
using to simulate the transport device has already been made available in the `alpen1` container,
so let's proceed with the last step of the transport process, which is to update the alpenhorn
data index to record the movement of the transport device.

There are generally, four fields we may need to upate for a transport device after it's been moved:
* its `host`, to let alpenhorn know which daemon should now be able to access the disk
* its `username` and `address` to set the log-in details for remote access to the device.  If
  remote access to the node isn't needed, this may not be necessary to do.
* its `root` to tell alpenhorn where we have mounted the transport device's filesystem.

We can do this all using the `node activate` command, which has been designed with this use
case in mind:
```
alpenhorn node activate transport1 --host=alpen1 --username=root --address=alpen1 \
                                   --root=/mnt/transport
```

The node will now appear to the daemon on `alpen1`:
```
alpen1-1  | Mar 09 04:21:44 INFO >> [MainThread] Node "transport1" now available.
alpen1-1  | Mar 09 04:21:44 INFO >> [MainThread] Group "transport_group" now available.
```

Let's now copy all the data off the transport media onto the `demo_storage1` node to
complete our transfer:
```
alpenhorn node sync --force transport1 demo_storage1
```

Once the transfers are complete, we've now got data back on `demo_storage1` courtesy
our transport media:
```(console)
root@alpen1:/# alpenhorn node stats
Name             File Count    Total Size    % Full
-------------  ------------  ------------  --------
demo_storage1             5         101 B         -
demo_storage2             5         101 B         -
demo_storage3             5         101 B         -
transport1                5         101 B         -
```

Once we're happy with the transfer off of the transport device, we'll want to clear it
out so we can ship it back to `alpen3` to be used to transfer more data in the future:

```
alpenhorn node clean --now --force transport1 --target=demo_storage1
```
The `--target` option ensures we only delete files from `transport1` which are present
on `demo_storage1`.
