package cluster

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/api/core/v1"
)

type Volume struct {
	Name       string
	Namespace  string
	ID         uint64
	Member     string
	IsCorrupt  bool
	IsAttached bool
}

func (v *Volume) etcdPVCName() string {
	return fmt.Sprintf("%s-pvc", v.Name)
}

type VolumeSet map[string]*Volume

func NewVolumeSet(vs ...*Volume) VolumeSet {
	res := VolumeSet{}
	for _, v := range vs {
		res[v.Name] = v
	}
	return res
}

// the set of all volumes of s1 that are not volumes of s2
func (vs VolumeSet) Diff(other VolumeSet) VolumeSet {
	diff := VolumeSet{}
	for n, v := range vs {
		if _, ok := other[n]; !ok {
			diff[n] = v
		}
	}
	return diff
}

// IsEqual tells whether two volume sets are equal by checking
// - they have the same set of volume and volume equality are judged by Name only.
func (vs VolumeSet) IsEqual(other VolumeSet) bool {
	if vs.Size() != other.Size() {
		return false
	}
	for n := range vs {
		if _, ok := other[n]; !ok {
			return false
		}
	}
	return true
}

func (vs VolumeSet) Size() int {
	return len(vs)
}

func (vs VolumeSet) String() string {
	var vstring []string

	for v := range vs {
		vstring = append(vstring, v)
	}
	return strings.Join(vstring, ",")
}

func (vs VolumeSet) PickOneAvailable() *Volume {
	for _, v := range vs {
		if !v.IsAttached {
			return v
		}
	}
	return nil
	//RETHINK panic("empty")
}

func (vs VolumeSet) Add(v *Volume) {
	vs[v.Name] = v
}

func (vs VolumeSet) Remove(name string) {
	delete(vs, name)
}

func CreateVolumeName(clusterName string, volume int) string {
	return fmt.Sprintf("%s-%04d", clusterName, volume)
}

func GetVolumeNameFromPVC(pvcName string) (string, error) {
	if strings.HasSuffix(pvcName, "-pvc") {
		return strings.TrimSuffix(pvcName, "-pvc"), nil
	}
	return "", fmt.Errorf("invalid PVC name")
}

/*
func podsToVolumeSet(pods []*v1.Pod) VolumeSet {
	volumes := etcdutil.VolumeSet{}
	for _, pod := range pods {
		pvc := pod.Spec.Volumes[0].VolumeSource.PersistentVolumeClaim
		v := &Volume{
			Name:       pvc.ClaimName,
			Namespace:  pod.Namespace,
			Member:     pod.Name,
			IsAttached: true,
		}
		volumes.Add(v)
	}
	return volumes
}
*/

func pvcsToVolumeSet(pvcs []*v1.PersistentVolumeClaim) VolumeSet {
	volumes := VolumeSet{}

	for _, pvc := range pvcs {
		name, err := GetVolumeNameFromPVC(pvc.Name)
		if err != nil {
			continue
		}
		v := &Volume{
			Name:       name,
			Namespace:  pvc.Namespace,
			IsAttached: false,
		}
		volumes.Add(v)
	}
	return volumes
}

func (c *Cluster) updateVolumes(known VolumeSet) {

	volumes := VolumeSet{}
	for _, v := range known {

		ct, err := GetCounterFromVolumeName(v.Name)
		if err != nil {
			//invalid volume name
			continue
		}
		if ct+1 > c.volumeCounter {
			c.volumeCounter = ct + 1
		}

		volumes[v.Name] = &Volume{
			Name:       v.Name,
			Namespace:  c.cluster.Namespace,
			IsAttached: v.IsAttached,
		}
	}
	c.volumes = volumes

	return
}

func GetCounterFromVolumeName(name string) (int, error) {
	i := strings.LastIndex(name, "-")
	if i == -1 || i+1 >= len(name) {
		return 0, fmt.Errorf("name (%s) does not contain '-' or anything after '-'", name)
	}
	c, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return 0, fmt.Errorf("could not atoi %s: %v", name[i+1:], err)
	}
	return c, nil
}
