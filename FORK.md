# Fork
This is a fork of [coreos/etcd-operator] developed for the [gardener/gardener] project.

# Rationale behind the fork

[gardener] project uses etcd-operator to manage the etcd instance storing the state of the kubernetes shoot clusters. The need for a more robust data storage option required etcd instances to store the state in a Persistent Volume than an EmptyDir. This requirement necessitated a fork of the operator to create and reconcile PV backed etcd instances.

- [coreos/etcd-operator] went through architectural change since v0.7.0 and has rendered the operator unsuitable for [gardener](at least on a short term).

- The scheduled backup and automated restore from latest backup in case of disaster is currently unavailable as a feature in upstream operator implementation since v0.7.0. Handling corner cases like effects of missing backup policy spec in the etcd CRD and lack of previous backups in cloud store to restore from in case of quorum failure was required to keep etcd instances available at all times.

- As per the discussion [here](https://github.com/coreos/etcd-operator/issues/1626), Coreos community has decided against supporting backup to object-stores on different cloud providers. This decision is one of the main reasons to fork the etcd-operator implementation.

- The PV support for etcd data directory has not been not merged to upstream implementation. This feature is mandatory from [gardener]'s perspective.

[md_references]: ""
[gardener/gardener]: https://github.com/gardener/gardener
[gardener]: https://github.com/gardener/gardener
[coreos/etcd-operator]: https://github.com/coreos/etcd-operator