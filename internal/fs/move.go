package fs

import (
	"context"
	"fmt"
	stdpath "path"
	"time"

	"github.com/OpenListTeam/OpenList/internal/driver"
	"github.com/OpenListTeam/OpenList/internal/errs"
	"github.com/OpenListTeam/OpenList/internal/model"
	"github.com/OpenListTeam/OpenList/internal/op"
	"github.com/OpenListTeam/OpenList/internal/task"
	"github.com/OpenListTeam/OpenList/pkg/utils"
	"github.com/pkg/errors"
	"github.com/xhofe/tache"
)

type MoveTask struct {
	task.TaskExtension
	Status       string        `json:"-"`
	SrcObjPath   string        `json:"src_path"`
	DstDirPath   string        `json:"dst_path"`
	srcStorage   driver.Driver `json:"-"`
	dstStorage   driver.Driver `json:"-"`
	SrcStorageMp string        `json:"src_storage_mp"`
	DstStorageMp string        `json:"dst_storage_mp"`
}

func (t *MoveTask) GetName() string {
	return fmt.Sprintf("move [%s](%s) to [%s](%s)", t.SrcStorageMp, t.SrcObjPath, t.DstStorageMp, t.DstDirPath)
}

func (t *MoveTask) GetStatus() string {
	return t.Status
}

func (t *MoveTask) Run() error {
	t.ReinitCtx()
	t.ClearEndTime()
	t.SetStartTime(time.Now())
	defer func() { t.SetEndTime(time.Now()) }()
	var err error
	if t.srcStorage == nil {
		t.srcStorage, err = op.GetStorageByMountPath(t.SrcStorageMp)
	}
	if t.dstStorage == nil {
		t.dstStorage, err = op.GetStorageByMountPath(t.DstStorageMp)
	}
	if err != nil {
		return errors.WithMessage(err, "failed get storage")
	}

	return moveBetween2Storages(t, t.srcStorage, t.dstStorage, t.SrcObjPath, t.DstDirPath)
}

var MoveTaskManager *tache.Manager[*MoveTask]


func moveBetween2Storages(t *MoveTask, srcStorage, dstStorage driver.Driver, srcObjPath, dstDirPath string) error {
	t.Status = "getting src object"
	srcObj, err := op.Get(t.Ctx(), srcStorage, srcObjPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] file", srcObjPath)
	}
	
	if srcObj.IsDir() {
		t.Status = "src object is dir, listing objs"
		objs, err := op.List(t.Ctx(), srcStorage, srcObjPath, model.ListArgs{})
		if err != nil {
			return errors.WithMessagef(err, "failed list src [%s] objs", srcObjPath)
		}
		
		dstObjPath := stdpath.Join(dstDirPath, srcObj.GetName())
		t.Status = "creating destination directory"
		err = op.MakeDir(t.Ctx(), dstStorage, dstObjPath)
		if err != nil {
			// Check if this is an upload-related error and provide a clearer message
			if errors.Is(err, errs.UploadNotSupported) {
				return errors.WithMessagef(err, "destination storage [%s] does not support creating directories", dstStorage.GetStorage().MountPath)
			}
			return errors.WithMessagef(err, "failed to create destination directory [%s] in storage [%s]", dstObjPath, dstStorage.GetStorage().MountPath)
		}
		
		for _, obj := range objs {
			if utils.IsCanceled(t.Ctx()) {
				return nil
			}
			srcSubObjPath := stdpath.Join(srcObjPath, obj.GetName())
			MoveTaskManager.Add(&MoveTask{
				TaskExtension: task.TaskExtension{
					Creator: t.GetCreator(),
				},
				srcStorage:   srcStorage,
				dstStorage:   dstStorage,
				SrcObjPath:   srcSubObjPath,
				DstDirPath:   dstObjPath,
				SrcStorageMp: srcStorage.GetStorage().MountPath,
				DstStorageMp: dstStorage.GetStorage().MountPath,
			})
		}

		t.Status = "cleaning up source directory"
		err = op.Remove(t.Ctx(), srcStorage, srcObjPath)
		if err != nil {
			t.Status = "completed (source directory cleanup pending)"
		} else {
			t.Status = "completed"
		}
		return nil
	} else {
		return moveFileBetween2Storages(t, srcStorage, dstStorage, srcObjPath, dstDirPath)
	}
}


func moveFileBetween2Storages(tsk *MoveTask, srcStorage, dstStorage driver.Driver, srcFilePath, dstDirPath string) error {
	tsk.Status = "copying file to destination"

	copyTask := &CopyTask{
		TaskExtension: task.TaskExtension{
			Creator: tsk.GetCreator(),
		},
		srcStorage:   srcStorage,
		dstStorage:   dstStorage,
		SrcObjPath:   srcFilePath,
		DstDirPath:   dstDirPath,
		SrcStorageMp: srcStorage.GetStorage().MountPath,
		DstStorageMp: dstStorage.GetStorage().MountPath,
	}
	copyTask.SetCtx(tsk.Ctx())

	err := copyBetween2Storages(copyTask, srcStorage, dstStorage, srcFilePath, dstDirPath)
	if err != nil {
		if errors.Is(err, errs.UploadNotSupported) {
			return errors.WithMessagef(err, "destination storage [%s] does not support file uploads", dstStorage.GetStorage().MountPath)
		}
		return errors.WithMessagef(err, "failed to copy [%s] to destination storage [%s]", srcFilePath, dstStorage.GetStorage().MountPath)
	}

	tsk.SetProgress(50)
	tsk.Status = "verifying file in destination"

	// check target files
	dstFilePath := stdpath.Join(dstDirPath, stdpath.Base(srcFilePath))
	const maxRetries = 3
	const retryInterval = time.Second
	var checkErr error
	for i := 0; i < maxRetries; i++ {
		_, checkErr = op.Get(tsk.Ctx(), dstStorage, dstFilePath)
		if checkErr == nil {
			break
		}
		time.Sleep(retryInterval)
	}
	if checkErr != nil {
		return errors.WithMessagef(checkErr, "file not found in destination [%s] after copy", dstFilePath)
	}

	tsk.Status = "deleting source file"
	err = op.Remove(tsk.Ctx(), srcStorage, srcFilePath)
	if err != nil {
		return errors.WithMessagef(err, "failed to delete src [%s] file from storage [%s] after successful copy", srcFilePath, srcStorage.GetStorage().MountPath)
	}

	tsk.SetProgress(100)
	tsk.Status = "completed"
	return nil
}

func _move(ctx context.Context, srcObjPath, dstDirPath string, lazyCache ...bool) (task.TaskExtensionInfo, error) {
	srcStorage, srcObjActualPath, err := op.GetStorageAndActualPath(srcObjPath)
	if err != nil {
		return nil, errors.WithMessage(err, "failed get src storage")
	}
	dstStorage, dstDirActualPath, err := op.GetStorageAndActualPath(dstDirPath)
	if err != nil {
		return nil, errors.WithMessage(err, "failed get dst storage")
	}

	if srcStorage.GetStorage() == dstStorage.GetStorage() {
		err = op.Move(ctx, srcStorage, srcObjActualPath, dstDirActualPath, lazyCache...)
		if !errors.Is(err, errs.NotImplement) && !errors.Is(err, errs.NotSupport) {
			return nil, err
		}
	}

	taskCreator, _ := ctx.Value("user").(*model.User)
	t := &MoveTask{
		TaskExtension: task.TaskExtension{
			Creator: taskCreator,
		},
		srcStorage:   srcStorage,
		dstStorage:   dstStorage,
		SrcObjPath:   srcObjActualPath,
		DstDirPath:   dstDirActualPath,
		SrcStorageMp: srcStorage.GetStorage().MountPath,
		DstStorageMp: dstStorage.GetStorage().MountPath,
	}
	MoveTaskManager.Add(t)
	return t, nil
}