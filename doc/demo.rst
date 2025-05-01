Alpenhorn Demo
==============

Introduction
------------

This is a short demonstration of using alpenhorn intended to show off
most of the major features of the system.

Demo Set-up
-----------

Because alpenhorn is designed to run as a distributed system, with both
data and software present at multiple dispersed, independent places,
this demo uses `docker <https://docs.docker.com/>`__ to run several
virtual images simulating independent hosts. Additionally, `Docker
Compose <https://docs.docker.com/compose/>`__ is used to manage the
multi-container set-up for this demo.

Installing docker itself is beyond the scope of this demo. The `Docker
install
documentation <https://docs.docker.com/get-started/get-docker/>`__ may
help. You may also be able to get help from your friendly neighbourhood
sysadmin. Once docker is properly installed you can test the
installation by running the ``hello-world`` container:

.. code:: console
   :class: demohost

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

.. hint::
   If you get a permission error: "permission denied while trying to connect
   to the Docker daemon socket", this means you lack the proper permissions to
   run docker.  The best solution in this case is to have your sysadmin add you
   to the ``docker`` group.  See `the Docker documentation on running docker as
   a non-root user
   <https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user>`__.

Once docker itself is running, you'll also need to `install the docker
compose plugin <https://docs.docker.com/compose/install/linux/>`__. After
installing docker compose, you should be able to run

.. code:: console
   :class: demohost

     docker compose version

and see the version of the plugin you just installed:

.. code:: console
   :class: demohost

   $ docker compose version
   Docker Compose version v2.31.0

If that's all working, you should be able to proceed with the alpenhorn
demo itself!

Starting the demo
-----------------

There are five docker containers that comprise this demo:

* a database container (``alpendb``), which runs the MySQL server containing
  the alpenhorn Data Index
* a container providing a root shell (``alpenshell``) used to interact
  with the alpenhorn CLI
* three containers (``alpenhost1`` through ``alpenhost3``) implementing the
  separate alpenhorn hosts, each containing a StorageNode and running an
  instance of the alpenhorn daemon.

These containers will be automatically built when we first start the
demo.

The demo must be run from the ``/demo/`` subdirectory of the alpenhorn
repository.  If you don't already have a clone of the alpenhorn repository,
the first step will be to clone the repository from GitHub:

.. code:: console
   :class: demohost

   git clone https://github.com/radiocosmology/alpenhorn.git

Once you've cloned the repository, you should change directory into the ``/demo/``
subdirectory of the newly-cloned repository (the directory containing
``Dockerfile.alpenhorn``):

.. code:: console
   :class: demohost

   $ git clone https://github.com/radiocosmology/alpenhorn.git
   Cloning into 'alpenhorn'...
   remote: Enumerating objects: 3764, done.
   remote: Counting objects: 100% (574/574), done.
   remote: Compressing objects: 100% (158/158), done.
   remote: Total 3764 (delta 444), reused 451 (delta 413), pack-reused 3190 (from 2)
   Receiving objects: 100% (3764/3764), 1.35 MiB | 1.35 MiB/s, done.
   Resolving deltas: 100% (2678/2678), done.
   $ cd alpenhorn/demo
   $ ls
   Dockerfile.alpenhorn  alpenhorn.conf  docker-compose.yaml

Once you're in the demo subdirectory, we can begin the demo.

Let's start off by starting the database container in the background.
Because alpenhorn is a distributed system, it is not expected that the
database itself runs on an alpenhorn node. We simulate this in the demo
by running the database out of a standard mysql container.

To start the database container, run the following from the ``/demo``
subdirectory:

.. code:: console
   :class: demohost

       docker compose up --detach alpendb

.. hint::
   If you get a ``no configuration file provided: not found`` error, you're
   not in the right directory. (The ``/demo/`` directory within the alpenhorn
   repository.)

Doing this the first time will probably cause docker to download the
latest MySQL image, create the virtual demo network and the ``demo_db_vol``
volume, which contains the persistent database for the demo:

.. code:: console
   :class: demohost

   $ docker compose up --detach alpendb
   [+] Running 11/11
    ✔ alpendb Pulled                                                           15.9s
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
   [+] Running 3/3
    ✔ Network demo_default      Created                                         0.1s
    ✔ Volume "demo_db_vol"      Created                                         0.0s
    ✔ Container demo-alpendb-1  Started                                         2.6s

You can use ``docker stats`` or ``docker container ls`` to verify that the
``alpendb`` container is running:

.. code:: console
   :class: demohost

   $ docker container ls
   CONTAINER ID   IMAGE          COMMAND         CREATED         STATUS         PORTS                 NAMES
   7e19895eb701   mysql:latest   "docker-ent…"   2 minutes ago   Up 2 minutes   3306/tcp, 33060/tcp   demo-alpendb-1

Stopping and resetting the demo
-------------------------------

.. tip::
   Before we continue, a few words about stopping and resetting this demo.

You can stop the docker containers running this demo at any time by
executing:

.. code:: console
   :class: demohost

   docker compose stop

This will stop all running containers. To restart the demo, run the
appropriate ``docker compose up`` commands. Stopping the demo does not
delete the containers or volumes containing the database and the storage
node data.

If you want to also remove the demo containers:

.. code:: console
   :class: demohost

   docker compose down --remove-orphans

To remove the containers *and* the volumes containing the database and
the storage node data:

.. code:: console
   :class: demohost

   docker compose down --remove-orphans --volumes

.. warning::
   Removing the volumes will delete the demo's alpenhorn data index.  After
   doing this, you'll need to rebuild the demo database from scratch as
   described below.

   Deleting the volumes will also delete all files in the StorageNodes which
   you create over the course of this demo.

Finally, to remove the alpenhorn container image, which gets built the
first time the image is nedded, run:

.. code:: console
   :class: demohost

   docker rmi alpenhorn:latest

You should do this if you want to update the version of alpenhorn used
by the demo, or if you've made changes to the demo's
``Dockerfile.alpenhorn`` or ``docker-compose.yaml`` files.

.. tip::
   You can also remove the ``mysql:latest`` image if you want to run a newer
   version of the database container.

Conventions used in this demo
-----------------------------

To follow along with this demo, you will be executing commands in three different
places:

* the docker host (the real machine on which you've cloned the alpenhorn repository)
* the ``alpenshell`` container, where you'll be issuing ``alpenhorn`` commands
* the ``alpenhost1`` container, where you'll be interacting with data files

To aide in distinguishing these three places, we've tried to indicate them by
using different highlights.

Commands you should execute on the docker host will look like this:

.. code:: console
   :class: demohost

   echo "This is a command on the demo host."

and command output will look like this:

.. code:: console
   :class: demohost

   $ echo "This is a command on the demo host."
   This is a command on the demo host

Commands meant to be run in the ``alpenshell`` container will look like this:

.. code:: console
   :class: demoshell

   echo "This is a command in the alpenshell container."

and command output will look like this:

.. code:: console
   :class: demoshell

   root@alpenshell:/# echo "This is a command in the alpenshell container."
   This is a command in the alpenshell container.

Finally, commands that need to be run in the ``alpenhost1`` container will look
like this:

.. code:: console
   :class: demonode1

   echo "This is a command in the alpenhost1 container."

and command output will look like this:

.. code:: console
   :class: demonode1

   root@alpenhost1:/# echo "This is a command in the alpenhost1 container."
   This is a command in the alpenhost1 container.

.. hint::
   How to access a shell in these containers is explained later on, when access
   to them is first needed.


Initialising the database
-------------------------

Now we need to use some ``alpenhorn`` commands to create the Data Index
(the alpenhorn database) and the define the start of our storage
infrastructure in it. The data index must exist before we can start the
first alpenhorn daemon.

To create the data index we'll need access to the MySQL database housing it.
This can't be done from the docker host, so we'll create a separate docker
container (called ``alpenshell``) which we'll use for the duration of this
demo to interact with alpenhorn.

To build the container and start a bash session in it, run:

.. code:: console
   :class: demohost

   docker compose run --rm alpenshell

.. note::
   The ``--rm`` option here means docker will delete the container when
   you exit it, preventing "orphan" containers.  If you forget to do this,
   and end up with warnings about orphan containers as a result, you can
   always add ``--remove-orphans`` to the command to remove the old containers.

Running this the first time will cause docker compose to build the
``alpenhorn`` container image. This may take some time. Eventually you
should be presented with a bash prompt as root inside the ``alpenshell``
container:

.. code:: console
   :class: demohost

   $ docker compose run --rm alpenshell
   [+] Creating 1/1
    ✔ Container demo-alpendb-1  Running                                                                                     0.0s
   [+] Running 1/1
    ! alpenshell Warning pull access denied for alpenhorn, repository does not exist or may require ...                     1.1s
   [+] Building 13.4s (4/15)                                                                                      docker:default
   [+] Building 79.8s (17/17) FINISHED                                                                            docker:default
    => [alpenshell internal] load build definition from Dockerfile.alpenhorn                                                0.0s
    => => transferring dockerfile: 1.20kB                                                                                   0.0s
    => [alpenshell internal] load metadata for docker.io/library/python:latest                                              1.2s
    => [alpenshell internal] load .dockerignore                                                                             0.0s
    => => transferring context: 2B                                                                                          0.0s
    => [alpenshell internal] load build context                                                                             0.9s
    => => transferring context: 5.97MB                                                                                      0.9s
    => [alpenshell  1/11] FROM docker.io/library/python:latest@sha256:c33390eacee652aecb774f9606c263b4f76415bc83926a6777e  18.8s
    => => resolve docker.io/library/python:latest@sha256:c33390eacee652aecb774f9606c263b4f76415bc83926a6777ede0f853c6bc19   0.0s
    => => sha256:ca513cad200b13ead2c745498459eed58a6db3480e3ba6117f854da097262526 64.39MB / 64.39MB                         1.8s
    => => sha256:c33390eacee652aecb774f9606c263b4f76415bc83926a6777ede0f853c6bc19 10.04kB / 10.04kB                         0.0s
    => => sha256:1dc5d6fc8bbd1dd9e0f4a202e99e03fe9575010057e730426c379da106ad446b 6.26kB / 6.26kB                           0.0s
    => => sha256:cf05a52c02353f0b2b6f9be0549ac916c3fb1dc8d4bacd405eac7f28562ec9f2 48.49MB / 48.49MB                         1.5s
    => => sha256:63964a8518f54dc31f8df89d7f06714c7a793aa1aa08a64ae3d7f4f4f30b4ac8 24.01MB / 24.01MB                         0.9s
    => => sha256:9ceebdae2d382eb0a06dfb69d15f21a14cb8dd4e369cc93df299fb4fd9c6183b 2.32kB / 2.32kB                           0.0s
    => => sha256:c187b51b626e1d60ab369727b81f440adea9d45e97a45e137fc318be0bb7f09f 211.36MB / 211.36MB                       4.7s
    => => sha256:776493ee5e4c0d0be79a520728d8e75ad7875d3d0a20c559719ce4bdbfd1135a 6.16MB / 6.16MB                           1.8s
    => => extracting sha256:cf05a52c02353f0b2b6f9be0549ac916c3fb1dc8d4bacd405eac7f28562ec9f2                                2.8s
    => => sha256:39ca2d92e12971b595d75bc8a5333312290333b9697057fbc650aa59b5e0d79f 27.38MB / 27.38MB                         2.6s
    => => sha256:ab89b311642188180787ced631a8b087ec24cc326cc76f84a4c2cd9cf30170a1 250B / 250B                               2.0s
    => => extracting sha256:63964a8518f54dc31f8df89d7f06714c7a793aa1aa08a64ae3d7f4f4f30b4ac8                                0.7s
    => => extracting sha256:ca513cad200b13ead2c745498459eed58a6db3480e3ba6117f854da097262526                                3.2s
    => => extracting sha256:c187b51b626e1d60ab369727b81f440adea9d45e97a45e137fc318be0bb7f09f                                7.8s
    => => extracting sha256:776493ee5e4c0d0be79a520728d8e75ad7875d3d0a20c559719ce4bdbfd1135a                                0.4s
    => => extracting sha256:39ca2d92e12971b595d75bc8a5333312290333b9697057fbc650aa59b5e0d79f                                1.0s
    => => extracting sha256:ab89b311642188180787ced631a8b087ec24cc326cc76f84a4c2cd9cf30170a1                                0.0
    => [alpenshell  2/11] RUN apt-get update && apt-get install --no-install-recommends -y     vim     ssh     rsync       14.3s
    => [alpenshell  3/11] RUN pip install --no-cache-dir mysqlclient                                                        8.0s
    => [alpenshell  4/11] RUN ssh-keygen -t rsa -N '' -f /root/.ssh/id_rsa                                                  1.2s
    => [alpenshell  5/11] RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys                                           0.5s
    => [alpenshell  6/11] RUN echo 'Host *\n    StrictHostKeyChecking no\n' > /root/.ssh/config                             0.6s
    => [alpenshell  7/11] COPY demo/alpenhorn.conf /etc/alpenhorn/alpenhorn.conf                                            0.1s
    => [alpenshell  8/11] RUN mkdir /var/log/alpenhorn                                                                      0.4s
    => [alpenshell  9/11] COPY examples/pattern_importer.py /root/python/pattern_importer.py                                0.1s
    => [alpenshell 10/11] ADD . /build                                                                                      0.4s
    => [alpenshell 11/11] RUN cd /build && pip install .                                                                   32.7s
    => [alpenshell] exporting to image                                                                                      1.2s
    => => exporting layers                                                                                                  1.2s
    => => writing image sha256:fd14160332396a1c20e3fc322dfa041887d0df81d362664be82fc2637df0e57c                             0.0s
    => => naming to docker.io/library/alpenhorn                                                                             0.0s
    => [alpenshell] resolving provenance for metadata file
    root@alpenshell:/#

Once at the root prompt, we can build the data index and start
populating it.

.. tip::
   You can log out of this ``alpenshell`` container at any time during the demo.  To later re-enter it,
   simply run the ``docker compose run --rm alpenshell`` command again.

Setting up the data index
~~~~~~~~~~~~~~~~~~~~~~~~~

Creating the data index is simple, and can be accomplished by running the following
command with the ``alpenhorn`` CLI utility:

.. code:: console
   :class: demoshell

   alpenhorn db init

.. hint::
   Remember that all these ``alpenhorn`` commands need to be run inside the
   ``alpenshell`` container that we started in the last section.

On successful completion, the ``db init`` command will report the version of the
database schema used to create the Data Index:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn db init
   Data Index version 2 initialised.

.. tip::
   It's worth pointing out at this point that the ``alpenhorn`` CLI can be run from
   anywhere that has access to the alpenhorn database.  It's explicitly not necessary
   to run the CLI on a host which contains a StorageNode (or is running the daemon),
   even when using the CLI to run commands which affect that StorageNode or daemon.

Setting up the import extension
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because alpenhorn is data agnostic, it doesn't have any facilities
out-of-the-box to import files. To be able to import files, alpenhorn
needs one or more "import-detect extensions" to be loaded. For the
purposes of this demo, we'll use the simple ``pattern_importer`` example
extension provided in the ``/examples`` directory. This extension has
already been incorporated into the alpenhorn container image that we're
running, and alpenhorn has been set up to use it.

.. hint::
   The reason alpenhorn is aware of the ``pattern_importer`` extension is
   because it is listed as an extension to load in the alpenhorn config file,
   which is available in the ``alpenshell`` at ``/etc/alpenhorn/alpenhorn.conf``.

   You can also take a look at it on the docker host, in the ``/demo/``
   subdirectory out of which you're running this demo.

As explained in the documentation for the ``pattern_importer`` example, the
extension adds four new tables to the alpenhorn Data Index: ``AcqData``,
``AcqType``, ``FileData``, and ``FileType``.  Adding extra tables to the Data
Index is permitted, but caution must be used to prevent name clashes with
alpenhorn's own tables, and tables from other potential extensions.
Fortunately, for the simple case in this demo, we don't have to worry about that.

To initialise the database for the extension, run the ``demo_init``
function provided by the extension:

.. code:: console
   :class: demoshell

   python -c 'import pattern_importer; pattern_importer.demo_init()'

If you get a ``ModuleNotFoundError: No module named 'pattern_importer'``
error, you're probably not executing this command in the root-shell in
the ``alpenshell`` container.

You should see a success message:

.. code:: console
   :class: demoshell

   root@alpenshell:/# python -c 'import pattern_importer; pattern_importer.demo_init()'
   Plugin init complete.

Create the first StorageNode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We need to start with a place to put some files. We'll create the first
`StorageNode`, which will be hosted on ``alpenhost1``. Before we can do
that, though we first need to create a `StorageGroup` to house the
node. Every `StorageNode` needs to be contained in a `StorageGroup`.
Typically each group contains only a single node, but certain group
classes support or require multiple nodes (such as the transport group
that we'll create later).

To create the group, which we'll call ``demo_storage1``, run:

.. code:: console
   :class: demoshell

   alpenhorn group create demo_storage1

This should create the group:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn group create demo_storage1
   Created storage group "demo_storage1".

.. hint::
   If instead you get an error: ``Error: Group "demo_storage1" already exists.``
   then likely you're trying to run this demo using an old instance of the database.
   In this case, you can stop the demo and delete the old database volume as
   explained above, if you want to start with a clean demo.

Now that the group is created, we can create a node within it. We'll
also call the node ``demo_storage1``. (By convention, when a
StorageGroup contains only one StorageNode, the node and group have the
same name, though that's not required.)

.. code:: console
   :class: demoshell

   alpenhorn node create demo_storage1 --group=demo_storage1 --auto-import --root=/data --host=alpenhost1

This command will create a new StorageNode called ``demo_storage1`` and
put it in the identically-named group. Auto-import (automatic monitoring for
new files) will be turned on; the mount point in the filesystem will be set
to ``/data`` and we declare it to be available on host ``alpenhost1``:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node create demo_storage1 --group=demo_storage1 --auto-import
                 --root=/data --host=alpenhost1
   Created storage node "demo_storage1".

That's enough to get us started.

.. tip::
   You will be issuing a lot of ``alpenhorn`` commands over the course of
   this demo. We suggest leaving the ``alpenshell`` prompt open to make it more
   convenient to issue them.  If you ever need to re-open the shell, remember
   you can run ``docker compose run alpenshell`` again to re-enter it.

Start the first daemon
----------------------

Now it's time to start the first daemon. The alpenhorn container image is
designed to run the alpenhorn daemon automatically. Start the first host container
by running the ``docker compose up`` command:

.. code:: console
   :class: demohost

   docker compose up --detach alpenhost1

Note: if you're following along with this demo, the database container
should already be running:

.. code:: console
   :class: demohost

   $ docker compose up --detach alpenhost1
   [+] Running 2/2
    ✔ Container demo-alpendb-1   Running                                                         0.0s
    ✔ Container demo-alpenhost1-1  Started                                                       0.4s

(If the database container is not running, docker compose will start it
first).

You should now check the logs for the daemon:

.. code:: console
   :class: demohost

   docker compose logs alpenhost1

(You can add ``--follow`` if you wish to have the logs continuously
update.) You'll see the alpenhorn daemon start up:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 00:38:32 INFO >> [MainThread] Alpenhorn start.
   alpenhost1-1  | Feb 21 00:38:32 INFO >> [MainThread] Loading config file /etc/alpenhorn/alpenhorn.conf
   alpenhost1-1  | Feb 21 00:38:32 INFO >> [MainThread] Loading extension pattern_importer
   alpenhost1-1  | Feb 21 00:38:32 INFO >> [Worker#1] Started.
   alpenhost1-1  | Feb 21 00:38:32 INFO >> [Worker#2] Started.

Two worker threads are started because that's what's specified in the
``alpenhornd.conf`` file. It has also loaded the ``pattern_exporter``
extension, since that's also specified in the config file.

Almost immediately, the daemon will notice that there are no *active*
nodes on ``alpenhost1``. It will perform this check roughly every ten
seconds, which is the update interval time set in the ``alpenhornd.conf`` file.

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 00:38:32 WARNING >> [MainThread] No active nodes on host (alpenhost1)!
   alpenhost1-1  | Feb 21 00:38:32 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost1-1  | Feb 21 00:38:32 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers
   alpenhost1-1  | Feb 21 00:38:42 WARNING >> [MainThread] No active nodes on host (alpenhost1)!
   alpenhost1-1  | Feb 21 00:38:42 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost1-1  | Feb 21 00:38:42 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers

We can fix this by activating the node we created.  To do this, in
the ``alpenshell`` container, we can use the ``node activate`` command:

.. code:: console
   :class: demoshell

   alpenhorn node activate demo_storage1

Alpenhorn will acknowledge the command:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node activate demo_storage1
   Storage node "demo_storage1" activated.

Now the daemon will find the active node, but there's still a problem:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 00:40:22 INFO >> [MainThread] Node "demo_storage1" now available.
   alpenhost1-1  | Feb 21 00:40:22 WARNING >> [MainThread] Node file "/data/ALPENHORN_NODE" could not be read.
   alpenhost1-1  | Feb 21 00:40:22 WARNING >> [MainThread] Ignoring node "demo_storage1": not initialised.
   alpenhost1-1  | Feb 21 00:40:22 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost1-1  | Feb 21 00:40:22 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers

We need to initialise the node so ``alpenhorn`` can use it. In this
case, we could do this by manually creating the ``/data/ALPENHORN_NODE``
file that it can't find. But, generally, it's easier to get alpenhorn
to initialise the node for us:

.. code:: console
   :class: demoshell

   alpenhorn node init demo_storage1

The initialisation is not performed by the alpenhorn CLI.  Instead the
CLI will create a request in the database to initialise the node:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node init demo_storage1
   Requested initialisation of Node "demo_storage1".

.. tip::
   A node only ever needs to be initialised once, when it is first created,
   but it's always safe to run this command: a request to initialise an
   already-initialised node is simply ignored.

The daemon on ``alpenhost1`` will notice this request and you should see the
node being initialised by one of the daemon workers:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 00:40:52 INFO >> [MainThread] Node "demo_storage1" now available.
   alpenhost1-1  | Feb 21 00:40:52 WARNING >> [MainThread] Node file "/data/ALPENHORN_NODE" could not be read.
   alpenhost1-1  | Feb 21 00:40:52 INFO >> [MainThread] Requesting init of node "demo_storage1".
   alpenhost1-1  | Feb 21 00:40:52 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost1-1  | Feb 21 00:40:52 INFO >> [Worker#1] Beginning task Init Node "demo_storage1"
   alpenhost1-1  | Feb 21 00:40:52 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
   alpenhost1-1  | Feb 21 00:40:52 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
   alpenhost1-1  | Feb 21 00:40:52 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
   alpenhost1-1  | Feb 21 00:40:52 INFO >> [Worker#1] Node "demo_storage1" initialised.
   alpenhost1-1  | Feb 21 00:40:52 INFO >> [Worker#1] Finished task: Init Node "demo_storage1"

After initialisation is complete, the daemon will finally be happy with
the Storage Node and start the auto-import monitor. The start of
auto-import triggers a "catch-up" job which searches for unknown,
pre-existing files that need import. As this is an empty node, though,
it won't find anything:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Node "demo_storage1" now available.
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Group "demo_storage1" now available.
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Watching node "demo_storage1" root "/data" for auto import.
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Beginning task Catch-up on demo_storage1
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Scanning "." on "demo_storage1" for new files.
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Scanning ".".
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [Worker#1] Finished task: Catch-up on demo_storage1
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Node demo_storage1: 46.77 GiB available.
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Updating node "demo_storage1".
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Updating group "demo_storage1".
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [MainThread] Tasks: 1 queued, 0 deferred, 0 in-progress on 2 workers

It will also run a job to see if there's anything needing clean-up on the
node. This "tidy up" job helps the alpenhorn daemon recover from
unexpected crashes by looking for and removing temporary files which the
alpenhorn daemon may have not been able to clean up the last time it ran.
The job is generally run when a node first becomes available to the daemon,
and then periodically after that. Again, because this is a brand-new node,
there isn't anything needing tidying:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 00:41:02 INFO >> [Worker#2] Beginning task Tidy up demo_storage1
   alpenhost1-1  | Feb 21 00:41:02 INFO >> [Worker#2] Finished task: Tidy up demo_storage1
   alpenhost1-1  | Feb 21 00:41:12 INFO >> [MainThread] Node demo_storage1: 46.77 GiB available.
   alpenhost1-1  | Feb 21 00:41:12 INFO >> [MainThread] Updating node "demo_storage1".
   alpenhost1-1  | Feb 21 00:41:12 INFO >> [MainThread] Updating group "demo_storage1".
   alpenhost1-1  | Feb 21 00:41:12 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost1-1  | Feb 21 00:41:12 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers

Importing files
---------------

Let's experiment now with importing files into alpenhorn, using both the
auto-import system and manually importing them.

What kind of files can be imported?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

As mentioned before, alpenhorn itself is agnostic to data file contents.
All decisions on which files are imported into the data index are made
by the import detect extensions, which can be tailored to the specific
data being managed. For this demo, the only import detect function we're
using is the example ``pattern_importer`` extension. This extension uses
a regular expressions to match against the pathnames of candidate files
to determine whether they should be imported or not.

The ``demo_init`` function that we called earlier to initialise the
database for this demo, added one allowed ArchiveAcq name pattern
consisting of a nested directory tree with the date: ``YYYY/MM/DD`` and
two allowed ArchiveFile name patterns. The first of these is a file
called "meta.txt" in the top acquisition directory
(i.e. ``YYYY/MM/DD/meta.txt``), which provides metadata for our notional
acquisition, and then data files with the time of day, sorted further
into hourly directories (i.e. ``YYYY/MM/DD/hh/mmss.dat``).

It bears repeating: the *contents* of these files are not interesting to
alpenhorn per se, but an import detect extension may be implemented
which inspects the data of the files being imported, if desired.

We'll continue this demo by creating files with the above-mentioned
naming conventions, without much concern about the file contents.

Auto-importing files and lock files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's start with auto-importing files. When auto-import is turned on for
a node, like it has been for our ``demo_storage1`` node, then files will
automatically be discovered by alpenhorn as they are added to the node
filesystem.

Care must be taken when writing files to a node filesystem when
auto-import is turned on to prevent alpenhorn from trying to import a
file before it is fully written. To prevent this from happening, before
creating a file on the node filesystem, we can create a *lock file*.

For a file at the path ``AAA/BBB/name.ext``, the corresponding lock file
will be called ``AAA/BBB/.name.ext.lock`` (i.e. the name of a lock file
is the name of the file it's locking plus a leading ``.`` and a
``.lock`` suffix.

Let's create the first file we want to import into alpenhorn, first
creating it's lockfile. To do this, we'll have to log into the ``alpenhost1``
container, to gain access to the ``demo_storage1`` filesystem.  We can
start a shell in the running container using ``docker exec``:

.. code:: console
   :class: demohost

   docker compose exec alpenhost1 bash -l


Once in this root shell on ``alpenhost1``, we can create the first of our files:

.. code:: console
   :class: demonode1

   cd /data
   mkdir -p 2025/02/21
   touch 2025/02/21/.meta.txt.lock
   echo "This is the first acquisition in the alpenhorn demo" > 2025/02/21/meta.txt


.. hint::
   If the ``cd`` command returns a "No such file or directory" error, then you're
   probably trying to create the file in the ``alpenshell`` container.  That container
   doesn't have access to the ``demo_storage1`` filesystem.  You need to create the
   files inside the ``alpenhost1`` container, which you can access using the
   ``docker compose exec`` command provided above.

When creating the file in this last step, you'll see alpenhorn notice
the file, but skip it because it's locked:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 23:04:21 INFO >> [Worker#1] Beginning task Import 2025/02/21/meta.txt on demo_storage1
   alpenhost1-1  | Feb 21 23:04:21 INFO >> [Worker#1] Skipping "2025/02/21/meta.txt": locked.
   alpenhost1-1  | Feb 21 23:04:21 INFO >> [Worker#1] Finished task: Import 2025/02/21/meta.txt on demo_storage1

.. note::
   In some cases file creation can cause multiple import requests to
   be scheduled. This is harmless: alpenhorn is prepared to handle multiple
   simultaneous attempts to import the same file and will only ever import
   a file once.

Once the file has been created, the lock file can be deleted, to trigger
import of the file:

.. code:: console
   :class: demonode1

   rm -f 2025/02/21/.meta.txt.lock

This will trigger alpenhorn to finally actually import the file:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Beginning task Import 2025/02/21/meta.txt on demo_storage1
   alpenhost1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Acquisition "2025/02/21" added to DB.
   alpenhost1-1  | Feb 21 23:07:07 INFO >> [Worker#1] File "2025/02/21/meta.txt" added to DB.
   alpenhost1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Imported file copy "2025/02/21/meta.txt" on node "demo_storage1".
   alpenhost1-1  | Feb 21 23:07:07 INFO >> [Worker#1] Finished task: Import 2025/02/21/meta.txt on demo_storage1

Note here that the the three lines in the middle of the daemon output
above indicate that the daemon has created three new records in the
database:

- an ``ArchiveAcq`` record for the new acquisition, with name ``2025/02/21``
- an ``ArchiveFile`` record for the new file, with name ``21/meta.txt``
  in the new acquisition
- an ``ArchiveFileCopy`` record recording that a copy of the newly-created
  ``ArchiveFile`` exists on ``demo_storage1``

You can use the alpenhorn CLI to see that this file is now present on
the ``demo_storage1`` node:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             1          52 B         -
   root@alpenshell:/# alpenhorn file list --node=demo_storage1 --details
   File                 Size    MD5 Hash                          Registration Time             State    Size on Node
   -------------------  ------  --------------------------------  ----------------------------  -------  --------------
   2025/02/21/meta.txt  52 B    4f2a66c1ff5eb90a5013522d53ea2e91  Fri Feb 21 23:07:08 2025 UTC  Healthy  4.000 kiB

Auto-importing files and temporary names
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Another option for writing files to a node filesystem when auto-import
is turned on, is to use a temporary name for the file which will cause
alpenhorn to decline to import the file. The import extensions which
you're using may provide a namespace for such files, as is the case with
this demo and the ``pattern_importer`` which has been configured: any
filename which does not match the patterns which were defined by the
``pattern_importer.demo_init`` function would work.

Whether or not your import extensions don't have provisions for omitting
files based on pathname, another option is to use a leading dot in the
filename of a file you're creating: alpenhorn will never import a file
whose first character is a ``.`` (dot). Note: this is only true of *file*
names: alpenhorn is still willing to import paths which contain
*directories* with leading dots in their names, assuming such names are
acceptable to at least one of your import extensions.

As an example, let's create a ``.dat`` file with a temporary name by
appending, say, ``.temp`` to the name of the file we want to create.
In the ``alpenhost1`` container:

.. code:: console
   :class: demonode1

   cd /data
   mkdir 2025/02/21/23
   echo "0 1 2 3 4 5" > 2025/02/21/23/1324.dat.temp

This file creation will be noticed by alpenhorn, but no import will
occur, because the ``pattern_exporter`` won't accept the name as valid:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 23:51:59 INFO >> [Worker#1] Beginning task Import 2025/02/21/23/1324.dat.temp on demo_storage1
   alpenhost1-1  | Feb 21 23:51:59 INFO >> [Worker#1] Not importing non-acquisition path: 2025/02/21/23/1324.dat.temp
   alpenhost1-1  | Feb 21 23:51:59 INFO >> [Worker#1] Finished task: Import 2025/02/21/23/1324.dat.temp on demo_storage1

The message "Not importing non-acquisition path" means no import
extension indicated to alpenhorn that the file should be imported. If,
instead, we had used a temporary filename with a leading dot, say,
``/data/2025/02/21/23/.1324.dat``, an import task wouldn't have even
been made, since alpenhorn would have rejected the file name earlier,
before it got around to attempting to import the file.

After file is fully written, it can be moved to the correct name. On
most filesystems, this is an atomic operation:

.. code:: console
   :class: demonode1

   mv 2025/02/21/23/1324.dat.temp 2025/02/21/23/1324.dat

.. hint::
   By "atomic operation" we mean: on most filesystems there is never
   a time during execution of the ``mv`` command when the destination
   filename ``2025/02/21/23/1324.dat`` refers to a partial file.  Either
   the destination file doesn't exist, or it exists and is complete.

This will trigger import of the file:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 21 23:52:20 INFO >> [Worker#2] Beginning task Import 2025/02/21/23/1324.dat on demo_storage1
   alpenhost1-1  | Feb 21 23:52:20 INFO >> [Worker#2] File "2025/02/21/23/1324.dat" added to DB.
   alpenhost1-1  | Feb 21 23:52:20 INFO >> [Worker#2] Imported file copy "2025/02/21/23/1324.dat" on node "demo_storage1".
   alpenhost1-1  | Feb 21 23:52:20 INFO >> [Worker#2] Finished task: Import 2025/02/21/23/1324.dat on demo_storage1

Unlike when we imported the first file, now only two new records are
created in the database, because the ``ArchiveAcq`` record already exists:

- an ``ArchiveFile`` for the new file
- an ``ArchiveFileCopy`` for the copy of the new file on ``demo_storage1``

Now there are two files on the node:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             2          64 B         -
   root@alpenshell:/# alpenhorn file list --node=demo_storage1 --details
   File                    Size    MD5 Hash                          Registration Time             State    Size on Node
   ----------------------  ------  --------------------------------  ----------------------------  -------  --------------
   2025/02/21/23/1324.dat  12 B    4c79018e00ddef11af0b9cfc14dd3261  Fri Feb 21 23:52:21 2025 UTC  Healthy  4.000 kiB
   2025/02/21/meta.txt     52 B    4f2a66c1ff5eb90a5013522d53ea2e91  Fri Feb 21 23:07:08 2025 UTC  Healthy  4.000 kiB

Manually importing files
~~~~~~~~~~~~~~~~~~~~~~~~

Let's now turn to the case where we *don't* have auto-import turned on
for a node. In this case there's no difficulty writing to the node,
since filesystem events won't trigger automatic attempts to import
files.

First, turn off auto-import on the node by modifying its properties:

.. code:: console
   :class: demoshell

   alpenhorn node modify demo_storage1 --no-auto-import

If you want, you can verify that auto-import has been turned off for the
node by checking its metadata after the ``modify`` command:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node modify demo_storage1 --no-auto-import
   Node updated.
   root@alpenshell:/# alpenhorn node show demo_storage1
      Storage Node: demo_storage1
     Storage Group: demo_storage1
            Active: Yes
              Type: -
             Notes:
         I/O Class: Default

       Daemon Host: alpenhost1
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

With that done, let's create some more data files:

.. code:: console
   :class: demonode1

   cd /data
   echo "0 1 2 3 4 5" > 2025/02/21/23/1330.dat
   echo "3 4 5 6 7 8" > 2025/02/21/23/1342.dat
   echo "9 10 11 12 13" > 2025/02/21/23/1349.dat

None of these files have been added to the database. We can use the
alpenhorn CLI to see this: as far as alpenhorn is concerned, there are
still only two files on the node.

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             2          64 B         -

But, now that we've finished writing these files, we can tell alpenhorn
to import them. This can be done for an individual file:

.. code:: console
   :class: demoshell

   alpenhorn file import --register-new 2025/02/21/23/1330.dat demo_storage1

.. hint::
   The ``--register-new`` flag tells alpenhorn that it is allowed to create
   a new ``ArchiveFile`` (and, were it necessary, an ``ArchiveAcq`` record,
   too) for newly discovered files. Without this flag, alpenhorn will only
   import files which are already represented by an existing
   ``ArchiveFile``. This second mode is more appropriate in cases where a
   node should not be receiving new files.

The CLI will create an import request for this file:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn file import --register-new 2025/02/21/23/1330.dat demo_storage1
   Added new import request.

The import request should be shortly handled by the daemon:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Beginning task Import 2025/02/21/23/1330.dat on demo_storage1
   alpenhost1-1  | Feb 22 00:09:36 INFO >> [Worker#1] File "2025/02/21/23/1330.dat" added to DB.
   alpenhost1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Imported file copy "2025/02/21/23/1330.dat" on node "demo_storage1".
   alpenhost1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Completed import request #2.
   alpenhost1-1  | Feb 22 00:09:36 INFO >> [Worker#1] Finished task: Import 2025/02/21/23/1330.dat on demo_storage1

It's also possible to tell alpenhorn to scan an entire directory for new
files:

.. code:: console
   :class: demoshell

   alpenhorn node scan demo_storage1 --register-new 2025/02/21

Which will add another import request:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node scan demo_storage1 --register-new 2025/02/21
   Added request for scan of "2025/02/21" on Node "demo_storage1".

Now alpenhorn will scan the requested path and find the other two files
we just created:

.. code:: console
   :class: demohost

   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Beginning task Scan "2025/02/21" on demo_storage1
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Scanning "2025/02/21" on "demo_storage1" for new files.
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Scanning "2025/02/21".
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Scanning "2025/02/21/23".
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#1] Beginning task Import 2025/02/21/23/1349.dat on demo_storage1
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Completed import request #4.
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Finished task: Scan "2025/02/21" on demo_storage1
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Beginning task Import 2025/02/21/23/1342.dat on demo_storage1
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#1] File "2025/02/21/23/1349.dat" added to DB.
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#1] Imported file copy "2025/02/21/23/1349.dat" on node "demo_storage1".
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] File "2025/02/21/23/1342.dat" added to DB.
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Imported file copy "2025/02/21/23/1342.dat" on node "demo_storage1".
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#1] Finished task: Import 2025/02/21/23/1349.dat on demo_storage1
   alpenhost1-1  | Feb 22 00:12:56 INFO >> [Worker#2] Finished task: Import 2025/02/21/23/1342.dat on demo_storage1

Now there are five files on the storage node:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             5         102 B         -

Syncing files between nodes
---------------------------

Let's now move on to syncing, or transferring, files between different
hosts.

Starting up the second and third nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before being able to transfer files, we need to create somewhere to
transfer them to. We'll start by creating the second storage node on the
second host:

.. code:: console
   :class: demoshell

   alpenhorn node create demo_storage2 --create-group --root=/data --host=alpenhost2

.. hint::
   The ``--create-group`` option to ``node create`` tells alpenhorn to also
   create a `StorageGroup` for the new node with the same name (i.e. the same
   thing we did manually for ``demo_storage1`` above)

This will create the second node:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node create demo_storage2 --create-group --root=/data --host=alpenhost2
   Created storage group "demo_storage2".
   Created storage node "demo_storage2".

Let's also make sure this node gets initialised, though this won't
happen immediately, since we haven't activated the Storage Node, nor are
we running the second daemon yet.

.. code:: console
   :class: demoshell

   alpenhorn node init demo_storage2

.. hint::
   Requests created by the alpenhorn CLI, be they initialisation requests,
   import requests, or transfer requests, do not require the target node to
   be active, nor do they require an alpenhorn daemon to be managing them.
   Requests made on inactive nodes will remain pending in the database
   until they can be handled by an alpenhorn daemon instance.

You can see pending requests, including this init request, using the
alpenhorn CLI:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node show demo_storage2 --all
      Storage Node: demo_storage2
     Storage Group: demo_storage2
            Active: No
              Type: -
             Notes:
         I/O Class: Default

       Daemon Host: alpenhost2
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

.. note::
   Node init requests are handled, under the hood, as a special kind of
   import request, which is why the Node Init request appears in the import
   request table.

This node is initially empty:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             5         102 B         -
   demo_storage2             0             -         -

Before starting transfers we have to record log-in details for the hosts
containing the nodes. alpenhorn uses SSH to log in to remote nodes when
performing transfers, meaning we need to specify a username and login-in
address for the node. For ``demo_storage1``, which is already active we
can do this by modifying the node record:

.. code:: console
   :class: demoshell

   alpenhorn node modify demo_storage1 --username root --address alpenhost1

For the second node, we can do it when we activate it. We could have
also specified these values when we created the node:

.. code:: console
   :class: demoshell

   alpenhorn node activate demo_storage2 --username root --address alpenhost2

.. tip::
   It's very important to distinguish the name used for a node's *host*
   (where the daemon managing the node is running) and the node's *address*
   (the name or IP address used by remote daemons to access the node via SSH).
   Often these two fields have the same value, but there's no requirement that
   they do.

Let's start up the second alpenhorn container to get the second node
running:

.. code:: console
   :class: demohost

   docker compose up --detach alpenhost2

You can monitor this nodes in the same way you did with alpenhost1:

.. code:: console
   :class: demohost

   docker compose logs alpenhost2

but it's also possible to monitor all nodes at once:

.. code:: console
   :class: demohost

   docker compose logs --follow

For now, the new node should initialise itself, and then idle: there are
no pending requests:

.. code:: console
   :class: demohost

   alpenhost2-1  | Feb 26 23:05:02 INFO >> [MainThread] Node "demo_storage2" now available.
   alpenhost2-1  | Feb 26 23:05:02 WARNING >> [MainThread] Node file "/data/ALPENHORN_NODE" could not be read.
   alpenhost2-1  | Feb 26 23:05:02 INFO >> [MainThread] Requesting init of node "demo_storage2".
   alpenhost2-1  | Feb 26 23:05:02 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost2-1  | Feb 26 23:05:02 INFO >> [MainThread] Tasks: 1 queued, 0 deferred, 0 in-progress on 2 workers
   alpenhost2-1  | Feb 26 23:05:02 INFO >> [Worker#1] Beginning task Init Node "demo_storage2"
   alpenhost2-1  | Feb 26 23:05:02 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
   alpenhost2-1  | Feb 26 23:05:02 WARNING >> [Worker#1] Node file "/data/ALPENHORN_NODE" could not be read.
   alpenhost2-1  | Feb 26 23:05:02 INFO >> [Worker#1] Node "demo_storage2" initialised.
   alpenhost2-1  | Feb 26 23:05:02 INFO >> [Worker#1] Finished task: Init Node "demo_storage2"
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [MainThread] Node "demo_storage2" now available.
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [MainThread] Group "demo_storage2" now available.
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [MainThread] Node demo_storage2: 45.51 GiB available.
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [MainThread] Updating node "demo_storage2".
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [MainThread] Updating group "demo_storage2".
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [MainThread] Tasks: 1 queued, 0 deferred, 0 in-progress on 2 workers
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [Worker#1] Beginning task Tidy up demo_storage2
   alpenhost2-1  | Feb 26 23:05:12 INFO >> [Worker#1] Finished task: Tidy up demo_storage2

Transferring a file
~~~~~~~~~~~~~~~~~~~

The alpenhorn daemon has the ability to transfer files between Storage
Nodes. To trigger file movement, we need to issue sync or transfer
requests. Transfer requests *always* request movement of a file from a
Storage Node into a Storage Group. Because all the groups we have for
now have a single node in them, this distinction isn't terribly
important, but we'll revisit this later, when we experiment with
multi-node groups.

We can transfer any existing file explicitly by issuing a transfer
request for it:

.. code:: console
   :class: demoshell

   alpenhorn file sync --from demo_storage1 --to demo_storage2 2025/02/21/meta.txt

This will submit a new transfer request:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn file sync --from demo_storage1 --to demo_storage2 2025/02/21/meta.txt
   Request submitted.

Transfers are always handled on the receiving side (that is: by the daemon
which considers the destination StorageGroup to be available). After, perhaps,
a short while, the daemon on ``alpenhost2`` will notice this request. First,
it will look at the local filesystem to see if the requested file
already exists. If it did, there would be no need for a transfer:

.. code:: console
   :class: demohost

   alpenhost2-1  | Feb 26 23:18:52 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/meta.txt in demo_storage2
   alpenhost2-1  | Feb 26 23:18:52 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/meta.txt in demo_storage2

But, in this case, the search will fail to find an existing copy of the
file, so then a file transfer will be started:

.. code:: console
   :class: demohost

   alpenhost2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Beginning task AFCR#1: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Creating directory "/data/2025/02/21".
   alpenhost2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Pulling remote file 2025/02/21/meta.txt using rsync
   alpenhost2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Pull of 2025/02/21/meta.txt complete. Transferred 52 B in 0.4s [139 B/s]
   alpenhost2-1  | Feb 26 23:18:52 INFO >> [Worker#1] Finished task: AFCR#1: demo_storage1 -> demo_storage2

.. note::
   The default tool for remote transfers is ``rsync``, but alpenhorn will
   also try to use `bbcp <https://www.slac.stanford.edu/~abh/bbcp/>`__, a
   GridFTP implementation, which may allow for higher-rate transfers, if it
   is available on to the daemon.

Now there is one file on ``demo_storage2``:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             5         102 B         -
   demo_storage2             1          52 B         -

You can check the filesystem on ``alpenhost2`` (by, say, running a ``find`` command)
to see that this file now exists on that node:

.. code:: console
   :class: demohost

   $ docker container run alpenhost2 find /data
   /data
   /data/ALPENHORN_NODE
   /data/2025
   /data/2025/02
   /data/2025/02/21
   /data/2025/02/21/meta.txt

Bulk transfers
~~~~~~~~~~~~~~

Rather than the tedious operation of requesting individual files to be
transferred, it is more typical to request *all* files present on a source
node and absent from a destination group be transferred:

.. code:: console
   :class: demoshell

   alpenhorn node sync demo_storage1 demo_storage2 --show-files

This will cause the alpenhorn CLI to create transfer requests for all
files which are present on ``demo_storage1`` but not present on
``demo_storage2``.

This command will require confirmation:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node sync demo_storage1 demo_storage2 --show-files
   Would sync 4 files (50 B) from Node "demo_storage1" to Group "demo_storage2":

   2025/02/21/23/1324.dat
   2025/02/21/23/1330.dat
   2025/02/21/23/1342.dat
   2025/02/21/23/1349.dat

   Continue? [y/N]: y

   Syncing 4 files (50 B) from Node "demo_storage1" to Group "demo_storage2".

   Added 4 new copy requests.

.. hint::
   Although there are five files on the node, only four of them will be
   transferred, because the first file we transferred is already on
   ``demo_storage2``.

The daemon on alpenhost2 will churn through these requests:

.. code:: console
   :class: demohost

   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/23/1330.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/23/1330.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Beginning task AFCR#2: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/23/1324.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Creating directory "/data/2025/02/21/23".
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/23/1324.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task AFCR#3: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pulling remote file 2025/02/21/23/1324.dat using rsync
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pulling remote file 2025/02/21/23/1330.dat using rsync
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [MainThread] Main loop execution was 0.1s.
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [MainThread] Tasks: 2 queued, 0 deferred, 2 in-progress on 2 workers
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pull of 2025/02/21/23/1324.dat complete. Transferred 12 B in 0.3s [36 B/s]
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pull of 2025/02/21/23/1330.dat complete. Transferred 12 B in 0.3s [36 B/s]
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: AFCR#3: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Finished task: AFCR#2: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task Pre-pull search for 2025/02/21/23/1349.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Beginning task Pre-pull search for 2025/02/21/23/1342.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: Pre-pull search for 2025/02/21/23/1349.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Beginning task AFCR#4: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Finished task: Pre-pull search for 2025/02/21/23/1342.dat in demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Beginning task AFCR#5: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pulling remote file 2025/02/21/23/1349.dat using rsync
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pulling remote file 2025/02/21/23/1342.dat using rsync
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Pull of 2025/02/21/23/1342.dat complete. Transferred 12 B in 0.4s [32 B/s]
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Pull of 2025/02/21/23/1349.dat complete. Transferred 14 B in 0.4s [37 B/s]
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#1] Finished task: AFCR#5: demo_storage1 -> demo_storage2
   alpenhost2-1  | Feb 26 23:34:32 INFO >> [Worker#2] Finished task: AFCR#4: demo_storage1 -> demo_storage2

And eventually all files will be transferred to ``alpenhost2``:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             5         102 B         -
   demo_storage2             5         102 B         -

.. hint::
   If you were to try the identical sync request a second time, after
   ``alpenhost2`` has finished all the transfers, alpenhorn will decide that
   nothing needs transferring and respond with "No files to sync".

One last note on the ``node sync`` command: if you prefer thinking about
the destination side of transfers, you can use ``group sync`` to perform
the same task.

The command

.. code:: console
   :class: demoshell

   alpenhorn node sync demo_storage1 demo_storage2 --show-files

is equivalent to

.. code:: console
   :class: demoshell

   alpenhorn group sync demo_storage2 demo_storage1 --show-files

though note that with ``node sync`` the arguments are source node and
then destination group but with ``group sync`` these are reversed: the
first argument is the destination group and the second argument the
source node.

Dealing with corruption
-----------------------

More than just helping you copy files around, alpenhorn can monitor your
files for corruption.

MD5 Digest Hashes
~~~~~~~~~~~~~~~~~

Although, as mentioned earlier, alpenhorn doesn't really know what's in
the files its managing, whenever it registers a new file, it computes
the MD5 digest hash for the file. This means that, if a file is changed
after registration, alpenhorn can detect this change by re-computing the
MD5 hash and comparing it to the hash value it recorded when first
registering the file.

You can see the stored hash value for a file using the alpenhorn CLI:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn file show 2025/02/21/23/1324.dat
          Name: 23/1324.dat
   Acquisition: 2025/02/21
          Path: 2025/02/21/23/1324.dat

          Size: 12 B
      MD5 Hash: 4c79018e00ddef11af0b9cfc14dd3261
    Registered: Thu Mar  6 22:54:37 2025 UTC

If we were to manually compute the MD5 digest for this file (in, say, the
``alpenhost1`` container) we would get the same result:

.. code:: console
   :class: demonode1

   root@alpenhost1:/data# md5sum 2025/02/21/23/1324.dat
   4c79018e00ddef11af0b9cfc14dd3261  2025/02/21/23/1324.dat

Let's corrupt a file by changing its contents on ``alpenhost1``:

.. code:: console
   :class: demonode1

   cd /data
   echo "bad data" > 2025/02/21/23/1324.dat

Now if we manually compute the MD5 hash, we can see that's it's
different than what alpenhorn has recorded:

.. code:: console
   :class: demonode1

   root@alpenhost1:/data# md5sum 2025/02/21/23/1324.dat
   3412f7b66a30b90ae3d3085c96615f00  2025/02/21/23/1324.dat

However, alpenhorn hasn't noticed this:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats --extra-stats
   Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
   -------------  ------------  ------------  --------  ---------------  ---------------  ---------------
   demo_storage1             5         102 B         -                -                -                -
   demo_storage2             5         102 B         -                -                -                -

It still lists no corrupt files on ``demo_storage1``. This is because
alpenhorn doesn't normally automatically detect corruption to files it
is managing. You can turn on "auto-verify" on a node, but that won't
result in instantaneous detection of corruption either, and can be I/O
expensive, (and, so, should be used with caution).

In some cases, file corruption will be detected by alpenhorn when copying
an unexpectedly corrupt file from one node to another. For now, we can
manually request a verification of the file. We'll do this by requesting
verification for the entire acquisition, even though we've only corrupted
one of the files.

To request verification of all files in the acquisition on the node
``demo_storage1``, run:

.. code:: console
   :class: demoshell

   alpenhorn node verify --all --acq=2025/02/21 demo_storage1

You will have to confirm this request:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node verify --all --acq=2025/02/21 demo_storage1
   Would request verification of 5 files (102 B).

   Continue? [y/N]: y

   Requesting verification of 5 files (102 B).
   Updated 5 files.

The daemon on ``alpenhost1`` will respond to this command by re-verifying
all files in that acquisition:

.. code:: console
   :class: demohost

   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Checking copy "2025/02/21/meta.txt" on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Checking copy "2025/02/21/23/1324.dat" on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Checking copy "2025/02/21/23/1330.dat" on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Checking copy "2025/02/21/23/1349.dat" on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Checking copy "2025/02/21/23/1342.dat" on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 ERROR >> [Worker#2] File 2025/02/21/23/1324.dat on node demo_storage1 is corrupt! Size: 9; expected: 12
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Updating file copy #2 for file 2025/02/21/23/1324.dat on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Updating group "demo_storage1".
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Finished task: Check file 2025/02/21/23/1324.dat on demo_storage1
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Beginning task Check file 2025/02/21/23/1330.dat on demo_storage1
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Main loop execution was 0.0s.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#1] File 2025/02/21/meta.txt on node demo_storage1 is A-OK!
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [MainThread] Tasks: 2 queued, 0 deferred, 2 in-progress on 2 workers
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Updating file copy #1 for file 2025/02/21/meta.txt on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Finished task: Check file 2025/02/21/meta.txt on demo_storage1
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Beginning task Check file 2025/02/21/23/1349.dat on demo_storage1
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] File 2025/02/21/23/1330.dat on node demo_storage1 is A-OK!
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Updating file copy #3 for file 2025/02/21/23/1330.dat on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Finished task: Check file 2025/02/21/23/1330.dat on demo_storage1
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Beginning task Check file 2025/02/21/23/1342.dat on demo_storage1
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#1] File 2025/02/21/23/1349.dat on node demo_storage1 is A-OK!
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Updating file copy #4 for file 2025/02/21/23/1349.dat on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] File 2025/02/21/23/1342.dat on node demo_storage1 is A-OK!
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Updating file copy #5 for file 2025/02/21/23/1342.dat on node demo_storage1.
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#1] Finished task: Check file 2025/02/21/23/1349.dat on demo_storage1
   alpenhost1-1  | Mar 07 01:48:25 INFO >> [Worker#2] Finished task: Check file 2025/02/21/23/1342.dat on demo_storage1

As you can see, it has discovered our corruption of ``2025/02/21/23/1324.dat``,
and also verified that the other files are not corrupt.

Now if we check the node stats, we can see one corrupt file on this
node.

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats --extra-stats
   Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
   -------------  ------------  ------------  --------  ---------------  ---------------  ---------------
   demo_storage1             4          90 B         -                1                -                -
   demo_storage2             5         102 B         -                -                -                -
   root@alpenshell:/# alpenhorn file state 2025/02/21/23/1324.dat demo_storage1
   Corrupt Ready

Also note that the file count for ``demo_storage1`` is down to four: a
known corrupt file is not considered "present" on a node, since it doesn't
provide the expected data.

Recovering corrupt files
------------------------

The standard way to recover a corrupt file copy is to re-transfer a
known-good copy of the file over top of the corrupt version. We can do
this by syncing the file back from ``alpenhost2``:

.. code:: console
   :class: demoshell

   alpenhorn node sync demo_storage2 demo_storage1

It will tell you there is only one file to transfer (the corrupt file)
and ask for confirmation:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node sync demo_storage2 demo_storage1
   Would sync 1 file (12 B) from Node "demo_storage2" to Group "demo_storage1".

   Continue? [y/N]: y

   Syncing 1 file (12 B) from Node "demo_storage2" to Group "demo_storage1".

   Added 1 new copy request.

Wait for the daemon on ``alpenhost1`` to pull the file from ``alpenhost2``:

.. code:: console
   :class: demohost

   alpenhost1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Beginning task AFCR#6: demo_storage2 -> demo_storage1
   alpenhost1-1  | Mar 07 01:52:15 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
   alpenhost1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Pulling remote file 2025/02/21/23/1324.dat using rsync
   alpenhost1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Pull of 2025/02/21/23/1324.dat complete. Transferred 12 B in 0.4s [32 B/s]
   alpenhost1-1  | Mar 07 01:52:15 INFO >> [Worker#1] Finished task: AFCR#6: demo_storage2 -> demo_storage1

After transferring the file back, now alpenhorn now considers the file
healthy again:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats --extra-stats
   Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
   -------------  ------------  ------------  --------  ---------------  ---------------  ---------------
   demo_storage1             5         102 B         -                -                -                -
   demo_storage2             5         102 B         -                -                -                -

Deleting files
--------------

Typically you'll want to delete files off your acquisition nodes once
they've been transferred off-site. File deletion can be accomplished
with the ``clean`` command.

Since we've copied some files from ``alpenhost1`` to ``alpenhost2``, let's try
deleting one of the files from ``alpenhost1``:

.. code:: console
   :class: demoshell

   alpenhorn file clean --now --node=demo_storage1 2025/02/21/meta.txt

The CLI should release the file immediately:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn file clean --now --node=demo_storage1 2025/02/21/meta.txt
   Released "2025/02/21/meta.txt" for immediate removal on Node "demo_storage1".

.. hint::
   The ``--now`` flag tells alpenhorn to delete the file as soon as
   possible. Without that flag, instead of being released for removal, the
   file is marked for "discretionary cleaning", which tells alpenhorn that
   it can decide to delete the file if it wants to clear space on the node,
   but in this demo alpenhorn would never decide to do that, so we'll opt
   for immediate removal.

Despite our request, if you look at the daemon log on ``alpenhost1``, you'll
see that it's refused to delete the file:

.. code:: console
   :class: demohost

   alpenhost1-1  | Mar 07 02:21:25 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
   alpenhost1-1  | Mar 07 02:21:25 WARNING >> [Worker#1] Too few archive copies (0) to delete 2025/02/21/meta.txt on demo_storage1.
   alpenhost1-1  | Mar 07 02:21:25 INFO >> [Worker#1] Finished task: Delete copies [1] from demo_storage1

To prevent data loss, alpenhorn will only delete file copies from a node
if at least two other copies of the file exist on other archive nodes.
Currently we have no archive nodes, so we can't delete files.

Let's fix that. While we do, the ``alpenhost1`` daemon will keep checking
whether it can delete that file.

Archive nodes
~~~~~~~~~~~~~

An archive node is any storage node with the "archive" storage type.
Let's change ``demo_storage2`` into an archive node. We do that by
modifying it's metadata:

.. code:: console
   :class: demoshell

   alpenhorn node modify --archive demo_storage2

After running this command, you can look at the node metadata to see
that it now has the "archive" storage type:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node modify --archive demo_storage2
   Node updated.
   root@alpenshell:/# alpenhorn node show demo_storage2
      Storage Node: demo_storage2
     Storage Group: demo_storage2
            Active: Yes
              Type: Archive
             Notes:
         I/O Class: Default

       Daemon Host: alpenhost2
    Log-in Address: alpenhost2
   Log-in Username: root

       Auto-Import: Off
       Auto-Verify: Off
         Max Total: -
         Available: 45.38 GiB
     Min Available: -
      Last Checked: Fri Mar  7 02:27:47 2025 UTC

   I/O Config:

     none

Now if we look at the ``alpenhost1`` daemon log, the file it's trying to
delete is now found on one archive node (out of the two needed):

.. code:: console
   :class: demohost

   alpenhost1-1  | Mar 07 02:28:55 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 1 in-progress on 2 workers
   alpenhost1-1  | Mar 07 02:28:55 WARNING >> [Worker#1] Too few archive copies (1) to delete 2025/02/21/meta.txt on demo_storage1.
   alpenhost1-1  | Mar 07 02:28:55 INFO >> [Worker#1] Finished task: Delete copies [1] from demo_storage1

We'll need another archive node with this file on it if we want the
deletion to happen. So, let's set up the final storage host, ``alpenhost3``.

First let's create the storage node in the database. We'll make this one
an archive node when we create it:

.. code:: console
   :class: demoshell

   alpenhorn node create demo_storage3 --create-group --archive --root=/data --host=alpenhost3 \
                                       --username root --address alpenhost3 --init --activate

.. tip::
   The ``--init`` and ``--activate`` flags save us from having to run those
   commands on the new node later.

Now let's start the third docker container and take a look at its logs:

.. code:: console
   :class: demohost

   docker compose up --detach alpenhost3
   docker compose logs --follow alpenhost3

Sync everything on ``demo_storage2`` to ``demo_storage3``:

.. code:: console
   :class: demoshell

   alpenhorn node sync --force demo_storage2 demo_storage3

.. caution::
   Using ``--force`` here skips the confirmation step. You can use
   ``--force`` with any alpenhorn command that would ask for confirmation,
   but you should be careful when using it.

As soon as the file is transferred to ``demo_storage3``, the daemon on
``alpenhost1`` will finally delete the file:

.. code:: console
   :class: demohost

   alpenhost1-1  | Mar 07 02:38:45 INFO >> [Worker#1] Beginning task Delete copies [1] from demo_storage1
   alpenhost1-1  | Mar 07 02:38:45 INFO >> [Worker#1] Removed file copy 2025/02/21/meta.txt on demo_storage1
   alpenhost1-1  | Mar 07 02:38:45 INFO >> [Worker#1] Finished task: Delete copies [1] from demo_storage1
   alpenhost1-1  | Mar 07 02:38:45 INFO >> [MainThread] Main loop execution was 0.1s.
   alpenhost1-1  | Mar 07 02:38:45 INFO >> [MainThread] Tasks: 0 queued, 0 deferred, 0 in-progress on 2 workers

Now there are only four files on ``demo_storage1``:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats --extra-stats
   Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
   -------------  ------------  ------------  --------  ---------------  ---------------  ---------------
   demo_storage1             4          50 B         -                -                -                -
   demo_storage2             5         102 B         -                -                -                -
   demo_storage3             5         102 B         -                -                -                -

As with sync requests, rather than cleaning individual files, we can do bulk
operations. To tell alpenhorn to delete everything from ``demo_storage1`` that
already exists on ``demo_storage3``:

.. code:: console
   :class: demoshell

   alpenhorn node clean demo_storage1 --now --target demo_storage3

It will find four files to clean, which you'll have to confirm:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node clean demo_storage1 --now --target demo_storage3
   Would release 4 files (50 B).

   Continue? [y/N]: y

   Releasing 4 files (50 B).
   Updated 4 files.

The files will be removed from ``demo_storage1`` by the daemon:

.. code:: console
   :class: demohost

   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Beginning task Delete copies [2, 3, 4, 5] from demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1324.dat on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1330.dat on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1349.dat on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed file copy 2025/02/21/23/1342.dat on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025/02/21/23 on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025/02/21 on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025/02 on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Removed directory /data/2025 on demo_storage1
   alpenhost1-1  | Mar 07 02:43:05 INFO >> [Worker#1] Finished task: Delete copies [2, 3, 4, 5] from demo_storage1

Note that the daemon will also delete directories on the node which end
up empty after file deletion to keep the storage node directory tree
tidy.

Now ``demo_storage1`` is empty:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats --extra-stats
   Name             File Count    Total Size    % Full    Corrupt Files    Suspect Files    Missing Files
   -------------  ------------  ------------  --------  ---------------  ---------------  ---------------
   demo_storage1             0             -         -                -                -                -
   demo_storage2             5         102 B         -                -                -                -
   demo_storage3             5         102 B         -                -                -                -

You can also inspect the filesystem on ``alpenhost`` to see that it is now empty:

.. code:: console
   :class: demonode1

   root@alpenhost1:/# find /data
   /data
   /data/ALPENHORN_NODE

Transport disks and the Sneakernet
----------------------------------

Alpenhorn has been designed to work with instruments in remote locations
where network transport of data may be difficult or impossible to
accomplish. To help with this situation, alpenhorn can be used to manage
transfer of data via physically moving storage media from site to site.
(This is known as the Sneakernet).

Alpenhorn can be configured to copy data onto a set of physical media at
one location where data are produced and then, later, copy data off
those media once they have been transported to a data ingest site.

To demonstrate this, we'll use a transport device to simulate
transferring data back from ``demo_storage3`` to ``demo_storage1``, as
if these two nodes were unable to communicate directly over the network.

The Transport Group and Transport Nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In alpenhorn, each individual physical device holding data to transfer
is represent by its own StorageNode which has the "transport" storage
type. All the transport nodes are collected into a StorageGroup which
has the "Transport" I/O class.

Our first job, then, is to create a transport group:

.. code:: console
   :class: demoshell

   alpenhorn group create --class=Transport transport_group

This has I/O class "Transport" (the capital "T" is important). Typically
you only ever need one transport group, and you put all your transport
nodes in the single group. Normal logistics of the Sneakernet mean that
typically different member nodes of this group will be located at
different sites and/or be in-transit at any given time, and the
locations of the nodes will change over time. Alpenhorn never requires,
nor expects, multiple nodes in the transport group to be accessible to a
single daemon.

Now that we have the transport group, we can create storage nodes to put
in it. As mentioned above, each node is a single physical device (disk,
tape, etc.) which is transferred through the Sneakernet. Multiple nodes
in the group can be available at a particular site, but we'll just
create a single node for the purpose of this demo.

When we create the new node, we'll tell alpenhorn that it's initially
available on ``alpenhost3``:

.. code:: console
   :class: demoshell

   alpenhorn node create transport1 --transport --group transport_group --host=alpenhost3 \
                                    --root=/mnt/transport --init --activate

Note the use of the ``--transport`` flag to set the node's storage type
to "transport". The "Transport" Group I/O class allows StorageNodes of
any class to be added to the group, but requires all such nodes to have
the "transport" storage type.

The filesystem has already been made available in the ``alpenhost3``
container, so wait for the daemon on ``alpenhost3`` to initialise the node:

.. code:: console
   :class: demohost

   alpenhost3-1  | Mar 07 09:21:03 INFO >> [MainThread] Node "transport1" now available.
   alpenhost3-1  | Mar 07 09:21:03 WARNING >> [MainThread] Node file "/mnt/transport/ALPENHORN_NODE" could not be read.
   alpenhost3-1  | Mar 07 09:21:03 INFO >> [MainThread] Requesting init of node "transport1".
   alpenhost3-1  | Mar 07 09:21:03 INFO >> [Worker#1] Beginning task Init Node "transport1"
   alpenhost3-1  | Mar 07 09:21:03 WARNING >> [Worker#1] Node file "/mnt/transport/ALPENHORN_NODE" could not be read.
   alpenhost3-1  | Mar 07 09:21:03 WARNING >> [Worker#1] Node file "/mnt/transport/ALPENHORN_NODE" could not be read.
   alpenhost3-1  | Mar 07 09:21:03 INFO >> [Worker#1] Node "transport1" initialised.

Copying Data to the Transport Group
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Remember that, when copy files, data always flows from a node to a
group. To get data onto the transport node, we need to transfer data
into the transport group. Logic defined by the Transport I/O class then
determines which of the available transport nodes the transferred files
will be written to.

Briefly, the Transport logic works like this:

* only local transfers are allowed into the group (i.e. syncing into
  the transport group will only ever copy data onto transport nodes at
  the same location as the source node).
* the transport group will try to fill up one transport node before
  putting data onto another
* all other things being equal, all transport nodes have the same
  priority for accepting data

In our case we only have a single transport node, so it's easy to figure
out which node the data will end up on.

Let's transfer data out of ``demo_storage3`` into the transport group
with the intent of transferring data, ultimately, to ``demo_storage1``:

.. code:: console
   :class: demoshell

   alpenhorn node sync demo_storage3 transport_group --target=demo_storage1

The ``--target`` option indicates to alpenhorn the ultimate destination
for the data we're syncing to transport. This prevents alpenhorn from
trying to transfer data already present on ``demo_storage1`` (though in
our case, that's nothing).

This should sync all five files we have:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node sync demo_storage3 transport_group --target=demo_storage1
   Would sync 5 files (102 B) from Node "demo_storage3" to Group "transport_group".

   Continue? [y/N]: y

   Syncing 5 files (102 B) from Node "demo_storage3" to Group "transport_group".

   Added 5 new copy requests.

It may also be good to point out here that even though both
``demo_storage3`` and the transport node are on ``alpenhost3``, and all the
resulting transfers are local, we'll still run this command in the
``alpenshell`` container.  Running commands with the CLI never need to occur
where the storage nodes referenced are. Anywhere that can access the alpenhorn
database can be used to run any alpenhorn command.

After waiting for the daemon to process these requests, a look at the
transport group should show us that they've all ended up on the
transport node:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn group show --node-stats transport_group
   Storage Group: transport_group
           Notes:
       I/O Class: Transport

   I/O Config:

     none

   Nodes:

   Name          File Count  Total Size    % Full
   ----------  ------------  ------------  --------
   transport1             5  102 B         -

Transporting the transport node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now let's simulate what would happen if we wanted to move this transport
node from ``alpenhost3`` to ``alpenhost1`` over the Sneakernet. (Normally, to
increase throughput of the Sneakernet, we would wait for the node to
fill up, but we're not going to wait for that in this demo.)

The first step is to deactivate the alpenhorn node to tell alpenhorn to
stop managing it:

.. code:: console
   :class: demoshell

   alpenhorn node deactivate transport1

The daemon on ``alpenhost3`` will notice this, and stop updating the
transport node:

.. code:: console
   :class: demohost

   alpenhost3-1  | Mar 09 04:10:07 INFO >> [MainThread] Node "transport1" no longer available.
   alpenhost3-1  | Mar 09 04:10:07 INFO >> [MainThread] Group "transport_group" no longer available.

.. hint::
   A StorageGroup is only available to a daemon if at least one of its nodes is
   available.  When we deactivate the ``transport1`` node, resulting it it no
   longer being available, the ``transport_group`` also becomes unavailable because
   there are no other active nodes on ``alpenhost3``.

If this were a real transport device, the next steps would be to:

- unmount the filesystem
- eject the media
- remove the physical storage device from the machine

After this, the transport device would need to travel (via Sneakernet) from
the site containing ``alpenhost3`` to the site containing ``alpenhost1`` where
we would do the reverse procedure, installing the device in the
``alpenhost1`` machine and mounting the device's filesystem.

For the purposes of this demo, however, we don't have to do any of that:
the docker volume we're using to simulate the transport device has
already been made available in the ``alpenhost1`` container, so let's
proceed with the last step of the transport process, which is to update
the alpenhorn data index to record the movement of the transport device.

In addition to activating the node to tell alpenhorn to start managing
it again, there are, generally, four fields we may need to update for a
transport device after it's been moved:

- its ``host``, to let alpenhorn know which daemon should now be able to access
  the disk
- its ``username`` and ``address``, to set the log-in details for remote access
  to the device. If remote access to the transport node isn't needed, this
  may not be necessary to do.
- its ``root``, to tell alpenhorn where we have mounted the transport device's
  filesystem.

We can do this all using the ``node activate`` command, which has been
designed with this use case in mind:

.. code:: console
   :class: demoshell

   alpenhorn node activate transport1 --host=alpenhost1 --username=root \
                           --address=alpenhost1 --root=/mnt/transport

The node (and also the group) will now appear to the daemon on ``alpenhost1``:

.. code:: console
   :class: demohost

   alpenhost1-1  | Mar 09 04:21:44 INFO >> [MainThread] Node "transport1" now available.
   alpenhost1-1  | Mar 09 04:21:44 INFO >> [MainThread] Group "transport_group" now available.

Now let's now copy all the data off the transport media onto the
``demo_storage1`` node to complete our long-distance transfer:

.. code:: console
   :class: demoshell

   alpenhorn node sync transport1 demo_storage1

Once the transfers are complete, we've now got data back on ``demo_storage1`` via our
transport media:

.. code:: console
   :class: demoshell

   root@alpenshell:/# alpenhorn node stats
   Name             File Count    Total Size    % Full
   -------------  ------------  ------------  --------
   demo_storage1             5         102 B         -
   demo_storage2             5         102 B         -
   demo_storage3             5         102 B         -
   transport1                5         102 B         -

Once we're happy with the transfer off of the transport device, we'll
want to clear it out so we can ship it back to ``alpenhost3`` to be used to
transfer more data in the future:

.. code:: console
   :class: demoshell

   alpenhorn node clean --now --force transport1 --target=demo_storage1

.. hint::
   The ``--target`` option ensures we only delete files from ``transport1``
   which are present on ``demo_storage1``.

Next steps
----------

This is the end of the curated part of the alpenhorn demo, but you can
use this demo system to experiment with running alpenhorn. Remember: you
can always reset this demo to its initial state.
