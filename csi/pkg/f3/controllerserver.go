/*
Copyright 2020 The Kubernetes Authors.

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
	"path/filepath"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/klog/v2"
)

// ControllerServer controller server setting
type ControllerServer struct {
	Driver *Driver
	// Working directory for the provisioner to temporarily mount f3 shares at
	workingMountDir string
}

// f3Volume is an internal representation of a volume
// created by the provisioner.
type f3Volume struct {
	// Volume id
	id string
	// Address of the F3 server.
	// Matches paramServer.
	server string
	// Base directory of the F3 server to create volumes under
	// Matches paramShare.
	baseDir string
	// Subdirectory of the F3 server to create volumes under
	subDir string
	// size of volume
	size int64
	// mount type (default is "f3")
	mountType string
}

// Ordering of elements in the CSI volume id.
// ID is of the form {server}/{baseDir}/{subDir}.
// TODO: This volume id format limits baseDir and
// subDir to only be one directory deep.
// Adding a new element should always go at the end
// before totalIDElements
const (
	idServer = iota
	idBaseDir
	idSubDir
	totalIDElements // Always last
)

// CreateVolume create a volume
func (cs *ControllerServer) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.Infof("CreateVolume ", req)
	
	// Use VolumeContentSource to specify the Ceph PVC we're layering ontop of

	// CreateVolume doesn't actually need to do anything, since F3 operates locally

	name := req.GetName()
	if len(name) == 0 {
		return nil, status.Error(codes.InvalidArgument, "CreateVolume name must be provided")
	}
	if err := cs.validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	reqCapacity := req.GetCapacityRange().GetRequiredBytes()
	f3Vol, err := cs.newF3Volume(name, reqCapacity, req.GetParameters())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	/*
	var volCap *csi.VolumeCapability
	if len(req.GetVolumeCapabilities()) > 0 {
		volCap = req.GetVolumeCapabilities()[0]
	}


	// Mount f3 base share so we can create a subdirectory
	if err = cs.internalMount(ctx, f3Vol, volCap); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount f3 server: %v", err.Error())
	}
	defer func() {
		if err = cs.internalUnmount(ctx, f3Vol); err != nil {
			klog.Warningf("failed to unmount f3 server: %v", err.Error())
		}
	}()

	// Create subdirectory under base-dir
	// TODO: revisit permissions
	internalVolumePath := cs.getInternalVolumePath(f3Vol)
	if err = os.Mkdir(internalVolumePath, 0777); err != nil && !os.IsExist(err) {
		return nil, status.Errorf(codes.Internal, "failed to make subdirectory: %v", err.Error())
	}
	*/
	// Remove capacity setting when provisioner 1.4.0 is available with fix for
	// https://github.com/kubernetes-csi/external-provisioner/pull/271
	return &csi.CreateVolumeResponse{Volume: cs.f3VolToCSI(f3Vol, reqCapacity)}, nil
}

// DeleteVolume delete a volume
func (cs *ControllerServer) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.Infof("DeleteVolume", req)

	return &csi.DeleteVolumeResponse{}, nil

	volumeID := req.GetVolumeId()
	if volumeID == "" {
		return nil, status.Error(codes.InvalidArgument, "volume id is empty")
	}
	f3Vol, err := cs.getF3VolFromID(volumeID)
	if err != nil {
		// An invalid ID should be treated as doesn't exist
		klog.Warningf("failed to get f3 volume for volume id %v deletion: %v", volumeID, err)
		return &csi.DeleteVolumeResponse{}, nil
	}

	// Mount f3 base share so we can delete the subdirectory
	if err = cs.internalMount(ctx, f3Vol, nil); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mount f3 server: %v", err.Error())
	}
	defer func() {
		if err = cs.internalUnmount(ctx, f3Vol); err != nil {
			klog.Warningf("failed to unmount f3 server: %v", err.Error())
		}
	}()

	// Delete subdirectory under base-dir
	internalVolumePath := cs.getInternalVolumePath(f3Vol)

	klog.V(2).Infof("Removing subdirectory at %v", internalVolumePath)
	if err = os.RemoveAll(internalVolumePath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete subdirectory: %v", err.Error())
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (cs *ControllerServer) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ControllerGetVolume(ctx context.Context, req *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if len(req.GetVolumeId()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID missing in request")
	}
	if req.GetVolumeCapabilities() == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities missing in request")
	}

	// supports all AccessModes, no need to check capabilities here
	return &csi.ValidateVolumeCapabilitiesResponse{Message: ""}, nil
}

func (cs *ControllerServer) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

// ControllerGetCapabilities implements the default GRPC callout.
// Default supports all capabilities
func (cs *ControllerServer) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: cs.Driver.cscap,
	}, nil
}

func (cs *ControllerServer) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (cs *ControllerServer) validateVolumeCapabilities(caps []*csi.VolumeCapability) error {
	if len(caps) == 0 {
		return fmt.Errorf("volume capabilities must be provided")
	}

	for _, c := range caps {
		if err := cs.validateVolumeCapability(c); err != nil {
			return err
		}
	}
	return nil
}

func (cs *ControllerServer) validateVolumeCapability(c *csi.VolumeCapability) error {
	if c == nil {
		return fmt.Errorf("volume capability must be provided")
	}

	// Validate access mode
	accessMode := c.GetAccessMode()
	if accessMode == nil {
		return fmt.Errorf("volume capability access mode not set")
	}
	if !cs.Driver.cap[accessMode.Mode] {
		return fmt.Errorf("driver does not support access mode: %v", accessMode.Mode.String())
	}

	// Validate access type
	accessType := c.GetAccessType()
	if accessType == nil {
		return fmt.Errorf("volume capability access type not set")
	}
	return nil
}

// Mount f3 server at base-dir
func (cs *ControllerServer) internalMount(ctx context.Context, vol *f3Volume, volCap *csi.VolumeCapability) error {
	sharePath := filepath.Join(string(filepath.Separator) + vol.baseDir)
	targetPath := cs.getInternalMountPath(vol)

	if volCap == nil {
		volCap = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				Mount: &csi.VolumeCapability_MountVolume{},
			},
		}
	}

	klog.V(4).Infof("internally mounting %v:%v at %v", vol.server, sharePath, targetPath)
	_, err := cs.Driver.ns.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{
		TargetPath: targetPath,
		VolumeContext: map[string]string{
			paramServer: vol.server,
			paramShare:  sharePath,
			paramType: vol.mountType,
		},
		VolumeCapability: volCap,
		VolumeId:         vol.id,
	})
	return err
}

// Unmount f3 server at base-dir
func (cs *ControllerServer) internalUnmount(ctx context.Context, vol *f3Volume) error {
	targetPath := cs.getInternalMountPath(vol)

	// Unmount f3 server at base-dir
	klog.V(4).Infof("internally unmounting %v", targetPath)
	_, err := cs.Driver.ns.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{
		VolumeId:   vol.id,
		TargetPath: cs.getInternalMountPath(vol),
	})
	return err
}

// Convert VolumeCreate parameters to an f3Volume
func (cs *ControllerServer) newF3Volume(name string, size int64, params map[string]string) (*f3Volume, error) {
	var (
		server  string
		baseDir string
		mountType string
	)

	// Validate parameters (case-insensitive).
	// TODO do more strict validation.
	for k, v := range params {
		switch strings.ToLower(k) {
		case paramServer:
			server = v
		case paramShare:
			baseDir = v
		case paramType:
			mountType = v
		default:
			return nil, fmt.Errorf("invalid parameter %q", k)
		}
	}

	/*
	// Validate required parameters
	if server == "" {
		return nil, fmt.Errorf("%v is a required parameter", paramServer)
	}
	if baseDir == "" {
		return nil, fmt.Errorf("%v is a required parameter", paramShare)
	}

	if mountType == "" {
		mountType = "f3"
	}*/

	vol := &f3Volume{
		server:  server,
		baseDir: baseDir,
		subDir:  name,
		size:    size,
		mountType: mountType,
	}
	//vol.id = cs.getVolumeIDFromF3Vol(vol)
	vol.id = name

	return vol, nil
}

// Get working directory for CreateVolume and DeleteVolume
func (cs *ControllerServer) getInternalMountPath(vol *f3Volume) string {
	// use default if empty
	if cs.workingMountDir == "" {
		cs.workingMountDir = "/tmp"
	}
	return filepath.Join(cs.workingMountDir, vol.subDir)
}

// Get internal path where the volume is created
// The reason why the internal path is "workingDir/subDir/subDir" is because:
//   * the semantic is actually "workingDir/volId/subDir" and volId == subDir.
//   * we need a mount directory per volId because you can have multiple
//     CreateVolume calls in parallel and they may use the same underlying share.
//     Instead of refcounting how many CreateVolume calls are using the same
//     share, it's simpler to just do a mount per request.
func (cs *ControllerServer) getInternalVolumePath(vol *f3Volume) string {
	return filepath.Join(cs.getInternalMountPath(vol), vol.subDir)
}

// Get user-visible share path for the volume
func (cs *ControllerServer) getVolumeSharePath(vol *f3Volume) string {
	return filepath.Join(string(filepath.Separator), vol.baseDir, vol.subDir)
}

// Convert into f3Volume into a csi.Volume
func (cs *ControllerServer) f3VolToCSI(vol *f3Volume, reqCapacity int64) *csi.Volume {
	return &csi.Volume{
		CapacityBytes: reqCapacity,
		VolumeId:      vol.id,
		VolumeContext: map[string]string{
			paramServer: vol.server,
			paramShare:  cs.getVolumeSharePath(vol),
			paramType:  vol.mountType,
		},
	}
}

// Given a f3Volume, return a CSI volume id
func (cs *ControllerServer) getVolumeIDFromF3Vol(vol *f3Volume) string {
	idElements := make([]string, totalIDElements)
	idElements[idServer] = strings.Trim(vol.server, "/")
	idElements[idBaseDir] = strings.Trim(vol.baseDir, "/")
	idElements[idSubDir] = strings.Trim(vol.subDir, "/")
	return strings.Join(idElements, "/")
}

// Given a CSI volume id, return a f3Volume
func (cs *ControllerServer) getF3VolFromID(id string) (*f3Volume, error) {
	tokens := strings.Split(id, "/")
	if len(tokens) != totalIDElements {
		return nil, fmt.Errorf("volume id %q unexpected format: got %v token(s) instead of %v", id, len(tokens), totalIDElements)
	}

	return &f3Volume{
		id:      id,
		server:  tokens[idServer],
		baseDir: tokens[idBaseDir],
		subDir:  tokens[idSubDir],
	}, nil
}
