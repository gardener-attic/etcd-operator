# Different use case with etcd pv

|Sr. No|Event|Pods|PVC|MemberSet|VolumeSet|Action|
|---|---|---|---|--|---|---|
|1| Single node, bootstrap|0|0|0|0|Bootstrap cluster.|
|2| Single node, reconcile|1|1|1|1|Bootstrap cluster.|
|3| Single node, only Pod failed/deleted|0|1|1|1|Restart pod.|
|4| Single node, PVC deleted|1|0|1|1|Update volumeset and wait till pod dies for disaster recovery.|
|5| Single node, PVC then Pod deleted| 0|0|1|1|Bootstrap with recovery.|
|6| Single node, operator pod goes down| -|-|0|0|Poll, update the data structures and check for all 1-4 cases|
|7| Single node, volume deleted on infra|-|-|1|1|No way to detect it currently. Restore cluster from backup.|

### TODO

Add multi node failure cases
