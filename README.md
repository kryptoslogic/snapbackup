# snapbackup

Snapbackup is a tool for migrating EC2 Snapshots from AWS to a remote server, over FTP or SFTP.

It does this by creating a volume for the snapshot, mounting it to the EC2 instance and tar-ing the files.

Once the migration is complete, the volume will be detached from the instance but not deleted -- this currently has to be done manually. 

Additionally, the snapshot will be added to a blacklist file. This is so that, when the tool is executed again, the operation is not repeated on this snapshot.

# Prerequisites 
The following binaries are required by the tool:
* mount
* umount
* tar
* lz4
* lsblk

Additionally, if you are using nvme drives, either *nvme* or *ebsnvme-id* is required to be installed.

# Disclaimer

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# Usage

## Migration Mode

* FTP
```bash
sudo ./snapbackup -hostname x.x.x.x:21 -ftp -username ftpuser -dir "/my/path/here" -ftppass "PASSWORD" -maxworkers 15
```

* SFTP 
```bash
sudo ./snapbackup -hostname x.x.x.x:22 -pkfile /home/ubuntu/.ssh/id_rsa -username sftpuser -dir "/my/path/here" -knownhostspath /home/ubuntu/.ssh/known_hosts -maxworkers 15
```

## Quick Verify Mode

* FTP
```bash
sudo ./snapbackup -hostname x.x.x.x:21 -ftp -username ftpuser -dir "/my/path/here" -ftppass "PASSWORD" -maxworkers 15 -quickverify
```

* SFTP 
```bash
sudo ./snapbackup -hostname x.x.x.x:22 -pkfile /home/ubuntu/.ssh/id_rsa -username sftpuser -dir "/my/path/here" -knownhostspath /home/ubuntu/.ssh/known_hosts -maxworkers 15 -quickverify
```



# Caveats (very important)

* If a partition has no FSTYPE, as reported by *lsblk*, then the operation on the partition is skipped and counted as successful.
* All snapshots are added to the blacklist once the operation on them has finished, regardless of whether or not the operation failed or was sucessful. If you wish to repeat the action on this snapshot, you must delete it manually from *snapblacklist.dat*
* If you wish to run the *quick verify mode* after you have migrated, you must delete the *snapbackup.dat* file first. Otherwise no snapshots will be verified.
* During internal testing, up-to 15 max workers seemed to run fine. 
* Currently, tar bottlenecks the speed of the transfer.
* If you are using SFTP mode, the hostname you are connecting to cannot be in the hashed format in your known_hosts file.
* Kernel panics have occured during testing, whilst detatching a volume from an EC2 instance that was running Ubuntu 16.04. This seemed to stop happening when the instance was upgraded to Ubuntu 18.04. If this occurs then volumes may have to be manually force-detached from the EC2 instance.

# Credits

This project has been heavily influenced by a similar tool, *snap-to-s3* by *thenickdude*.

https://github.com/thenickdude/snap-to-s3
