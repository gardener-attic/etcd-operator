# Different use case with etcd pv

|Sr. No|Event|Pods|PVC|MemberSet|VolumeSet|Action|
|---|---|---|---|--|---|---|
|1| Single node, bootstrap|0|0|0|0|bootstrap|
|2| Single node, reconcile|1|1|1|1|bootstrap|
|3| Single node, only Pod failed/deleted|0|1|1|1|restart pod|
|4| Single node, PVC deleted|1|0|1|1|probably restore or let it be and update columeset|
|5|Single node, PVC then Pod deleted| 0|0|1|1|bootstrap with recovery|
|6| SIngle node, operator goes down| -|-|0|0|poll and updated data structure again check for all 1-4 cases|
|7| Single node, volume deleted on infra|-|-|1|1|restore|

### TODO

Add multi node failure cases
