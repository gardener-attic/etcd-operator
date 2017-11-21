# Backup Options Config Guide

In etcd operator, we provide the following options to save cluster backups to:
- Persistent Volume (PV) on GCE or AWS
- Persistent Volume (PV) with custom StorageClasses
- S3 bucket on AWS
- Azure Blob Storage (ABS) container
- OpenStack Swift Container

This docs talks about how to configure etcd operator to use these backup options.

## PV with custom StorageClass

If your Kubernetes supports the [StorageClass](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#storageclasses) resource, you can use them to back up your etcd cluster. To do this, specify a `StorageClass` value in the cluster's Backup spec, like so:

```yaml
spec:
  ...
  backup:
    ...
    storageType: "PersistentVolume"
    pv:
      volumeSizeInMB: 512
      storageClass: foo
```

This spec field provides more granular control over how to persist etcd data to PersistentVolumes. This is essentially saving backups to a PersistentVolume with a predefined StorageClass.

## PV on GCE

This is essentially saving backups to an instance of GCE PD if running on GCE Kubernetes.

Create your own storage class with the provisioner for GCE PD:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: storage-class-gce-pd
provisioner: kubernetes.io/gce-pd
```
Then specify the name of the above storage class in the cluster backup spec field `spec.backup.pv.storageClass`.

## PV on AWS

This is essentially saving backups on an instance of AWS EBS if running on AWS Kubernetes.

Create your own storage class with the provisioner for AWS EBS:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: storage-class-aws-ebs
provisioner: kubernetes.io/aws-ebs
```
Then specify the name of the above storage class in the cluster backup spec field `spec.backup.pv.storageClass`.

## S3 on AWS

The S3 backup policy is configured in a cluster's spec. 

See the [S3 backup with cluster specific configuration](spec_examples.md#s3-backup-and-cluster-specific-s3-configuration) spec to see what the cluster's `spec.backup` field should be configured as to set a cluster specific S3 backup configuration. The following additional fields need to be set under the cluster spec's `spec.backup.s3` field:
- `s3Bucket`: The name of the S3 bucket to store backups in.
- `awsSecret`: The secret object name which should contain two files named `credentials` and `config` .
- `prefix`: (Optional) The S3 [prefix](http://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html).

The profile to use in both the files `credentials` and `config` is `default` :
```
$ cat ~/.aws/credentials
[default]
aws_access_key_id = XXX
aws_secret_access_key = XXX

$ cat ~/.aws/config
[default]
region = us-west-1
```

We can then create the secret named "aws" from the two files by:
```bash
$ kubectl -n <namespace-name> create secret generic aws --from-file=$AWS_DIR/credentials --from-file=$AWS_DIR/config
```

Once the secret is created, it can be used to configure a new cluster or update an existing one with the specific S3 configurations:
```
spec:
  backup:
    s3:
      s3Bucket: example-s3-bucket
      awsSecret: aws
      prefix: example-prefix
```

For AWS k8s users: If `credentials` file is not given,
operator and backup sidecar pods will make use of AWS IAM roles on the nodes where they are deployed.

## ABS on Azure

The ABS backup policy is configured in a cluster's spec.  See [spec_examples.md](spec_examples.md#three-member-cluster-with-abs-backup) for an example.

### Prerequisites

  * An ABS container will need to be created in Azure. Here we name the container `etcd-backups`.

    ```
    $ export AZURE_STORAGE_ACCOUNT=<storage-account-name> AZURE_STORAGE_KEY=<storage-key>
    $ az storage container create -n etcd-backups
    ```

  * A Kubernetes secret will need to be created.

      An example of the secret manifest should look like:
      ```
      apiVersion: v1
      kind: Secret
      metadata:
        name: abs-credentials
      type: Opaque
      stringData:
        storage-account: <storage-account-name>
        storage-key: <storage-key>
      ```

      To create the secret from the secret manifest:
      ```
      $ kubectl -n <namespace> create -f secret-abs-credentials.yaml
      ```

What we have:
- A secret "abs-credentials"
- An ABS container "etcd_backups"

### Cluster configuration

The following fields need to be set under the cluster spec's `spec.backup.abs` field:
- `absContainer`: The name of the ABS container to store backups in.
- `absSecret`: The secret object name (as created above.)

An example cluster with specific ABS configurations then looks like:
```
spec:
  backup:
    storageType: "ABS"
    abs:
      absContainer: etcd-backups
      absSecret: abs-credentials
```

## OpenStack Swift Container
The Swift backup policy is configured in a cluster's spec.  See [spec_examples.md](spec_examples.md#three-member-cluster-with-swift-backup) for an example.

### Prerequisites

  * A container will need to be created in Swift. Here we name the container `etcd-backups`.

    ```
    $ openstack container create <container-name>
    ```

  * A Kubernetes secret will need to be created.

      An example of the secret manifest should look like:
      ```
      apiVersion: v1
      kind: Secret
      metadata:
        name: openstack-swift-credentials
      type: Opaque
      data:
        identityEndpoint: <openstack-identity-auth-URL>
        domainName: <openstack-domain-name>
        username: <swift-user-name>
        password: <swift-password>
        tenantID: <openstack-project-id>
      ```

      To create the secret from the secret manifest:
      ```
      $ kubectl -n <namespace> create -f openstack-swift-credentials.yaml
      ```

What we have:
- A secret "openstack-swift-credentials"
- An openstack swift container "etcd_backups"

### Cluster configuration

The following fields need to be set under the cluster spec's `spec.backup.swift` field:
- `swiftContainer`: The name of the openstack swift container to store backups in.
- `swiftSecret`: The secret object name (as created above.)
- `swiftRegion`: The name of the region in which openstack swift is deployed e.g. "eu-de-1"

An example cluster with specific Swift configurations then looks like:
```
spec:
  backup:
    storageType: "Swift"
    swift:
      swiftContainer: etcd-backups
      swiftSecret: openstack-swift-credentials
      swiftRegion: <swift-region>
```

