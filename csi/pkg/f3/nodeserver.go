/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package f3

import (
	"fmt"
	"os"
	"strings"
	"path"
	//"bytes"
	"os/exec"
	"io/ioutil"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/utils/mount"
)

// NodeServer driver
type NodeServer struct {
	Driver  *Driver
	mounter mount.Interface
	fuseProcs map[string]*exec.Cmd
}

// NodePublishVolume mount the volume
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	klog.Infof("NodePublishVolume", req)

	// This is where we actually run the FUSE driver
	// At this point the Ceph volume must be mounted on the host
	// We need to know where that mount point is and use that as the "subdir" arg for the FUSE driver
	// The "tempdir" arg should be set by the F3 storage class (accessible via the Volume req?)
	// The "workdir" will be the target path

	// Need to save the pid of the driver process so we can kill it on Unpublish

	if req.GetVolumeCapability() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability missing in request")
	}
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path not provided")
	}

	if _, err := os.Stat(targetPath); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	podUUID := req.GetVolumeContext()["csi.storage.k8s.io/pod.uid"]
	podDir := path.Join("/var/lib/kubelet/pods/", podUUID, "volumes/kubernetes.io~csi/")

	files, err := ioutil.ReadDir(podDir)
	if err != nil {
		klog.Infof("XXX uh")
	}
 
	cephMount := ""
	for _, f := range files {
		klog.Infof(f.Name())
		// XXX assuming that there's only two volumes attached, and if it's not us
		// it must be the Ceph volume
		if f.Name() != volumeID {
			cephMount = path.Join(podDir, f.Name(), "mount")
			klog.Infof("Ceph mount: ", cephMount)
		}
	}

	if cephMount == "" {
		klog.Infof("Ceph not mounted yet!  Gotta wait...")
		return nil, status.Error(codes.Unavailable, "Ceph not mounted yet")

	}

	if _, err := os.Stat(cephMount); err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.Unavailable, "Ceph mount doesn't exist yet")
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	klog.Infof("Running ", "/fusetest" + " " + cephMount + " " + targetPath)
	//cmd := exec.Command("/fusetest", cephMount, targetPath)
	cmd := exec.Command("/fusetest", "--nosplice", "--debug", "--nocache", "--address", ns.Driver.nodeID+":9999", "--idroot", ns.Driver.tempdir, "--socket-path", ns.Driver.socketAddress, cephMount, targetPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		klog.Infof(err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}
	ns.fuseProcs[volumeID] = cmd

	/*
	go func(cephMount, targetPath string) {
		klog.Infof("Running ", "/fusetest" + " " + cephMount + " " + targetPath)
		cmd := exec.Command("/fusetest", cephMount, targetPath)
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Start(); err != nil {
			klog.Infof(err.Error())
		} else {
			ns.fuseProcs[volumeID] = cmd

			err := cmd.Wait()
			
			// If Wait ever finishes it means the FUSE process crashed before we
			// unpublished the volume...
			klog.Infof("Got error ", err)
			klog.Infof("stdout", stdout.String())
			klog.Infof("stderr", stderr.String())

			delete(ns.fuseProcs, volumeID)
		}
	}(cephMount, targetPath)
	*/

	return &csi.NodePublishVolumeResponse{}, nil

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(targetPath, 0750); err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
			notMnt = true
		} else {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if !notMnt {
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if acquired := ns.Driver.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
	}
	defer ns.Driver.volumeLocks.Release(volumeID)

	mountOptions := req.GetVolumeCapability().GetMount().GetMountFlags()
	if req.GetReadonly() {
		mountOptions = append(mountOptions, "ro")
	}

	s := req.GetVolumeContext()[paramServer]
	ep := req.GetVolumeContext()[paramShare]
	mt := req.GetVolumeContext()[paramType]
	source := fmt.Sprintf("%s:%s", s, ep)

	klog.V(2).Infof("NodePublishVolume: volumeID(%v) source(%s) targetPath(%s) mountflags(%v) mounttype(%v)", volumeID, source, targetPath, mountOptions, mt)
	err = ns.mounter.Mount(source, targetPath, mt, mountOptions)
	//err = ns.mounter.Mount(source, targetPath, "f3", mountOptions)
	if err != nil {
		if os.IsPermission(err) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		if strings.Contains(err.Error(), "invalid argument") {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	if ns.Driver.perm != nil {
		if err := os.Chmod(targetPath, os.FileMode(*ns.Driver.perm)); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume unmount the volume
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	klog.Infof("NodeUnpublishVolume", req)

	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	targetPath := req.GetTargetPath()
	if len(targetPath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Target path missing in request")
	}

	// XXX what order do we need to do this in? (umount first or kill FUSE first?)
	if cmd, exists := ns.fuseProcs[volumeID]; exists {
		if err := cmd.Process.Kill(); err != nil {
			if err.Error() == "process already finished" {
				klog.Infof("FUSE process already exited")
				delete(ns.fuseProcs, volumeID)
			} else {
				klog.Infof("failed to kill FUSE proc ", err.Error())
				return nil, status.Error(codes.Internal, err.Error())
			}
		}
		delete(ns.fuseProcs, volumeID)
	} else {
		klog.Infof("volumeID not in map, FUSE not running? Nothing to do...")
		delete(ns.fuseProcs, volumeID)
	}

	cmd := exec.Command("umount", targetPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		klog.Infof("Failed to umount", err.Error())
		klog.Infof(string(out))
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil

	notMnt, err := ns.mounter.IsLikelyNotMountPoint(targetPath)

	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, "Targetpath not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	if acquired := ns.Driver.volumeLocks.TryAcquire(volumeID); !acquired {
		return nil, status.Errorf(codes.Aborted, volumeOperationAlreadyExistsFmt, volumeID)
	}
	defer ns.Driver.volumeLocks.Release(volumeID)

	klog.V(2).Infof("NodeUnpublishVolume: CleanupMountPoint %s on volumeID(%s)", targetPath, volumeID)
	err = mount.CleanupMountPoint(targetPath, ns.mounter, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeGetInfo return info of the node on which this plugin is running
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.Driver.nodeID,
	}, nil
}

// NodeGetCapabilities return the capabilities of the Node plugin
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: ns.Driver.nscap,
	}, nil
}

// NodeGetVolumeStats get volume stats
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, req *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	klog.Infof("NodeGetVolumeStats", req)

	return nil, status.Error(codes.Unimplemented, "")

	if len(req.VolumeId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume ID was empty")
	}
	if len(req.VolumePath) == 0 {
		return nil, status.Error(codes.InvalidArgument, "NodeGetVolumeStats volume path was empty")
	}

	_, err := os.Stat(req.VolumePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Errorf(codes.NotFound, "path %s does not exist", req.VolumePath)
		}
		return nil, status.Errorf(codes.Internal, "failed to stat file %s: %v", req.VolumePath, err)
	}

	volumeMetrics, err := volume.NewMetricsStatFS(req.VolumePath).GetMetrics()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get metrics: %v", err)
	}

	available, ok := volumeMetrics.Available.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform volume available size(%v)", volumeMetrics.Available)
	}
	capacity, ok := volumeMetrics.Capacity.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform volume capacity size(%v)", volumeMetrics.Capacity)
	}
	used, ok := volumeMetrics.Used.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform volume used size(%v)", volumeMetrics.Used)
	}

	inodesFree, ok := volumeMetrics.InodesFree.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform disk inodes free(%v)", volumeMetrics.InodesFree)
	}
	inodes, ok := volumeMetrics.Inodes.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform disk inodes(%v)", volumeMetrics.Inodes)
	}
	inodesUsed, ok := volumeMetrics.InodesUsed.AsInt64()
	if !ok {
		return nil, status.Errorf(codes.Internal, "failed to transform disk inodes used(%v)", volumeMetrics.InodesUsed)
	}

	return &csi.NodeGetVolumeStatsResponse{
		Usage: []*csi.VolumeUsage{
			{
				Unit:      csi.VolumeUsage_BYTES,
				Available: available,
				Total:     capacity,
				Used:      used,
			},
			{
				Unit:      csi.VolumeUsage_INODES,
				Available: inodesFree,
				Total:     inodes,
				Used:      inodesUsed,
			},
		},
	}, nil
}

// NodeUnstageVolume unstage volume
func (ns *NodeServer) NodeUnstageVolume(ctx context.Context, req *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// NodeStageVolume stage volume
func (ns *NodeServer) NodeStageVolume(ctx context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

// NodeExpandVolume node expand volume
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func makeDir(pathname string) error {
	err := os.MkdirAll(pathname, os.FileMode(0755))
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	return nil
}
