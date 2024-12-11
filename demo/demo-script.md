Alpenhorn
=========

Preliminaries
------------


1. Start the DB and enter the first container
    ```
    docker compose up --remove-orphans --detach db
    docker compose run host_1 bash -l
    ```

2. Initialize Alpenhorn database

    ```
    python -c 'import pattern_importer; pattern_importer.demo_init()'
    alpenhorn db init
    alpenhorn group create group_1
    alpenhorn node create node_1 --auto-import --root=/data --group=group_1 --host=host_1
    alpenhorn node activate node_1 --username root --address host_1
    ```


Auto-Importing files
--------------------

3. Start the service
    ```
    docker compose up --remove-orphans --detach host_1
    docker compose logs host_1
    ```
   
   Note files being imported. 
   (Could also do `docker compose logs host_1 | head -30`.)
   
4. Client
    ```
    docker compose exec host_1 bash -l
    alpenhorn node stats
    ```
   
   Two files on node_1:

   ```
   root@host_1:/# alpenhorn node stats
   Name      File Count    Total Size    % Full
   ------  ------------  ------------  --------
   node_1             2          15 B         -
   ```

   ```
   root@host_1:/# alpenhorn file list --node=node_1 --details
File                                                                                  Size    MD5 Hash                          Registration Time             State    Size on Node
------------------------------------------------------------------------------------  ------  --------------------------------  ----------------------------  -------  --------------
2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/raw/acq_123_1.zxc  15 B    9ef14b877f34ed94c8dc3e700db49dad  Wed Jan 22 22:14:03 2025 UTC  Healthy  4.000 kiB
2017/03/21/acq_xy1_45678901T000000Z_inst_zab/summary.txt                              0 B     d41d8cd98f00b204e9800998ecf8427e  Wed Jan 22 22:14:03 2025 UTC  Healthy  0 B
   ```


Start more nodes
---------------

5. Create storage

    ```
    alpenhorn group create group_2
    alpenhorn node create node_2 --auto-import --root=/data --group=group_2 --host=host_2
    alpenhorn node activate node_2 --username root --address host_2

    alpenhorn group create group_3
    alpenhorn node create node_3 --auto-import --root=/data --group=group_3 --host=host_3
    alpenhorn node activate node_3 --username root --address host_3
    ```

6. Start the service
    ```
    docker compose up -d host_1
    docker compose up host_2
    docker compose up host_3
    ```
   
   Note no files are being imported.
   

Syncing files
---------------

7. Client
    ```
    docker compose exec host_2 bash -l
    find /data
    ```
   
    No files on host_2

8. Start a sync
    ```
    alpenhorn node sync node_1 group_2 --show_files
    ```
    
    Note the server copying

    ```
    find /data
    ```
    The files are here!

9. Sync an explicit acquisition
    ```
    docker compose exec host_3 bash -l
    find /data
    alpenhorn sync node_1 group_3 --acq=12345678T000000Z_inst_zab --show_files
    ```
    
    Note the server copying


10. Client
    ```
    alpenhorn status
    alpenhorn verify node_2
    alpenhorn verify node_3
    ```
   
   Two files on host_2, one file on host_3. 
   

Add a new file
--------------

11. create a new file
    ```
    docker compose exec host_2 bash -l
    echo foo bar > /data/12345678T000000Z_inst_zab/jim.out
    ```

12. see the new file get synced
    ```
    alpenhorn sync --acq 12345678T000000Z_inst_zab node_2 group_3 --show_files
    alpenhorn status
    docker compose up host_3
    ```
    
    Highlight "transferring file ... jim.out"
    
13. See how many files are present
    ```
    alpenhorn status
    ```

Dealing with corruption
-----------------------

14. Modify a copy of a file:
    ```
    alpenhorn status
    echo bla >> /data/12345678T000000Z_inst_zab/jim.out
    alpenhorn status
    ```

    Notice in the output of the service:

    ```
    > INFO >> Checking file "/data/12345678T000000Z_inst_zab/jim.out" on node "node_2".
    > ERROR >> File is corrupted!
    > INFO >> Updating file copy status [id=6].
    ```

15. Repair with a sync from a known good copy:

    ```
    alpenhorn sync --acq 12345678T000000Z_inst_zab node_1 group_2 --show_files
    alpenhorn status
    ```

Cleaning
--------

16. Remove unneeded files with `clean`:
    ```
    alpenhorn clean --now node_2
    ```

    Observe only two are removed: "Too few backups to delete
    2017/03/21/acq_xy1_45678901T000000Z_inst_zab/acq_data/x_123_1_data/raw/acq_123_1.zxc".
    This is because this acquisition was never synced to node_3


Transport disks
---------------

17. Create storage

    ```
    alpenhorn create_group transport
    alpenhorn create_node --storage_type=T t1 /mnt/t1 host_1 transport
    ```

18. Make it available

    ```
    alpenhorn mount --address=host_1 t1
    ```

19. Copy to transport for a target destination:

    ```
    alpenhorn sync node_1 transport --target group_3 --show_files
    ```
    
    Note files being synced
    ```
    alpenhorn status
    find /mnt/t1
    alpenhorn unmount t1
    ```
    
20. On the other side (host_3)...
    ```
    alpenhorn mount --address=host_3 t1
    alpenhorn sync t1 group_3 --show_files
    ```

    Note files being synced
    ```
    alpenhorn status
    alpenhorn clean --now t1
    alpenhorn status
    find /mnt/t1
    ```
