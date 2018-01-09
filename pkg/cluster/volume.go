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

// PickOneAvailable will pick volume structure which is not attached to any member.
// Here we don't care whether it's in sync with PVC. i.e. volume attached or unattached
// at the execution of this function call may or may not have associated PVC
func (vs VolumeSet) PickOneAvailable() *Volume {
	for _, v := range vs {
		if !v.IsAttached {
			fmt.Printf("Volume getting attached: %v", v)
			return v
		}
	}
	return nil
	//RETHINK panic("empty")
}

//Add adds volume to volumeset
func (vs VolumeSet) Add(v *Volume) {
	vs[v.Name] = v
}

//Remove removes volume from volumeset
func (vs VolumeSet) Remove(name string) {
	delete(vs, name)
}

func createVolumeName(clusterName string, volume int) string {
	return fmt.Sprintf("%s-%04d-pvc", clusterName, volume)
}

func pvcsToVolumeSet(pvcs []*v1.PersistentVolumeClaim) VolumeSet {
	volumes := VolumeSet{}

	for _, pvc := range pvcs {
		v := &Volume{
			Name:       pvc.Name,
			Namespace:  pvc.Namespace,
			IsAttached: false,
		}
		volumes.Add(v)
	}
	return volumes
}

func (c *Cluster) updateVolumes(known VolumeSet) {
	c.volumes = VolumeSet{}
	for _, v := range known {
		c.logger.Infof("PVC name: %v", v.Name)
		ct, err := getCounterFromVolumeName(v.Name)
		if err != nil {
			//invalid volume name
			c.logger.Errorf("skipping pvc %s cause :", v.Name, err)
			continue
		}
		if ct+1 > c.volumeCounter {
			c.volumeCounter = ct + 1
		}
		c.volumes.Add(v)

	}

	for _, m := range c.members {
		if c.volumes[m.Volume] != nil {
			c.attachVolumeToMember(c.volumes[m.Volume], m)
		}
	}
	return
}

func getCounterFromVolumeName(name string) (int, error) {
	if !strings.HasSuffix(name, "-pvc") {
		return -1, fmt.Errorf("invalid volume name")
	}
	name = strings.TrimSuffix(name, "-pvc")
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
