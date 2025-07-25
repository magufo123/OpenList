package fs

import (
	"context"
	"fmt"
	stdpath "path"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/OpenList/v4/internal/task_group"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/OpenListTeam/tache"
	"github.com/pkg/errors"
)

type taskType uint8

func (t taskType) String() string {
	if t == 0 {
		return "copy"
	} else {
		return "move"
	}
}

const (
	copy taskType = iota
	move
)

type FileTransferTask struct {
	TaskData
	TaskType taskType
	groupID  string
}

func (t *FileTransferTask) GetName() string {
	return fmt.Sprintf("%s [%s](%s) to [%s](%s)", t.TaskType, t.SrcStorageMp, t.SrcActualPath, t.DstStorageMp, t.DstActualPath)
}

func (t *FileTransferTask) Run() error {
	if err := t.ReinitCtx(); err != nil {
		return err
	}
	t.ClearEndTime()
	t.SetStartTime(time.Now())
	defer func() { t.SetEndTime(time.Now()) }()
	var err error
	if t.SrcStorage == nil {
		t.SrcStorage, err = op.GetStorageByMountPath(t.SrcStorageMp)
	}
	if t.DstStorage == nil {
		t.DstStorage, err = op.GetStorageByMountPath(t.DstStorageMp)
	}
	if err != nil {
		return errors.WithMessage(err, "failed get storage")
	}
	return putBetween2Storages(t, t.SrcStorage, t.DstStorage, t.SrcActualPath, t.DstActualPath)
}

func (t *FileTransferTask) OnSucceeded() {
	task_group.TransferCoordinator.Done(t.groupID, true)
}

func (t *FileTransferTask) OnFailed() {
	task_group.TransferCoordinator.Done(t.groupID, false)
}

func (t *FileTransferTask) SetRetry(retry int, maxRetry int) {
	t.TaskExtension.SetRetry(retry, maxRetry)
	if retry == 0 &&
		(len(t.groupID) == 0 || // 重启恢复
			(t.GetErr() == nil && t.GetState() != tache.StatePending)) { // 手动重试
		t.groupID = stdpath.Join(t.DstStorageMp, t.DstActualPath)
		var payload any
		if t.TaskType == move {
			payload = task_group.SrcPathToRemove(stdpath.Join(t.SrcStorageMp, t.SrcActualPath))
		}
		task_group.TransferCoordinator.AddTask(t.groupID, payload)
	}
}

func transfer(ctx context.Context, taskType taskType, srcObjPath, dstDirPath string, lazyCache ...bool) (task.TaskExtensionInfo, error) {
	srcStorage, srcObjActualPath, err := op.GetStorageAndActualPath(srcObjPath)
	if err != nil {
		return nil, errors.WithMessage(err, "failed get src storage")
	}
	dstStorage, dstDirActualPath, err := op.GetStorageAndActualPath(dstDirPath)
	if err != nil {
		return nil, errors.WithMessage(err, "failed get dst storage")
	}

	if srcStorage.GetStorage() == dstStorage.GetStorage() {
		if taskType == copy {
			err = op.Copy(ctx, srcStorage, srcObjActualPath, dstDirActualPath, lazyCache...)
			if !errors.Is(err, errs.NotImplement) && !errors.Is(err, errs.NotSupport) {
				return nil, err
			}
		} else {
			err = op.Move(ctx, srcStorage, srcObjActualPath, dstDirActualPath, lazyCache...)
			if !errors.Is(err, errs.NotImplement) && !errors.Is(err, errs.NotSupport) {
				return nil, err
			}
		}
	} else if ctx.Value(conf.NoTaskKey) != nil {
		return nil, fmt.Errorf("can't %s files between two storages, please use the front-end ", taskType)
	}

	// if ctx.Value(conf.NoTaskKey) != nil { // webdav
	// 	srcObj, err := op.Get(ctx, srcStorage, srcObjActualPath)
	// 	if err != nil {
	// 		return nil, errors.WithMessagef(err, "failed get src [%s] file", srcObjPath)
	// 	}
	// 	if !srcObj.IsDir() {
	// 		// copy file directly
	// 		link, _, err := op.Link(ctx, srcStorage, srcObjActualPath, model.LinkArgs{})
	// 		if err != nil {
	// 			return nil, errors.WithMessagef(err, "failed get [%s] link", srcObjPath)
	// 		}
	// 		// any link provided is seekable
	// 		ss, err := stream.NewSeekableStream(&stream.FileStream{
	// 			Obj: srcObj,
	// 			Ctx: ctx,
	// 		}, link)
	// 		if err != nil {
	// 			_ = link.Close()
	// 			return nil, errors.WithMessagef(err, "failed get [%s] stream", srcObjPath)
	// 		}
	// 		if taskType == move {
	// 			defer func() {
	// 				task_group.TransferCoordinator.Done(dstDirPath, err == nil)
	// 			}()
	// 			task_group.TransferCoordinator.AddTask(dstDirPath, task_group.SrcPathToRemove(srcObjPath))
	// 		}
	// 		err = op.Put(ctx, dstStorage, dstDirActualPath, ss, nil, taskType == move)
	// 		return nil, err
	// 	} else {
	// 		return nil, fmt.Errorf("can't %s dir two storages, please use the front-end ", taskType)
	// 	}
	// }

	// not in the same storage
	taskCreator, _ := ctx.Value(conf.UserKey).(*model.User)
	t := &FileTransferTask{
		TaskData: TaskData{
			TaskExtension: task.TaskExtension{
				Creator: taskCreator,
				ApiUrl:  common.GetApiUrl(ctx),
			},
			SrcStorage:    srcStorage,
			DstStorage:    dstStorage,
			SrcActualPath: srcObjActualPath,
			DstActualPath: dstDirActualPath,
			SrcStorageMp:  srcStorage.GetStorage().MountPath,
			DstStorageMp:  dstStorage.GetStorage().MountPath,
		},
		TaskType: taskType,
		groupID:  dstDirPath,
	}
	if taskType == copy {
		task_group.TransferCoordinator.AddTask(dstDirPath, nil)
		CopyTaskManager.Add(t)
	} else {
		task_group.TransferCoordinator.AddTask(dstDirPath, task_group.SrcPathToRemove(srcObjPath))
		MoveTaskManager.Add(t)
	}
	return t, nil
}

func putBetween2Storages(t *FileTransferTask, srcStorage, dstStorage driver.Driver, srcActualPath, dstDirActualPath string) error {
	t.Status = "getting src object"
	srcObj, err := op.Get(t.Ctx(), srcStorage, srcActualPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] file", srcActualPath)
	}
	if srcObj.IsDir() {
		t.Status = "src object is dir, listing objs"
		objs, err := op.List(t.Ctx(), srcStorage, srcActualPath, model.ListArgs{})
		if err != nil {
			return errors.WithMessagef(err, "failed list src [%s] objs", srcActualPath)
		}
		dstActualPath := stdpath.Join(dstDirActualPath, srcObj.GetName())
		if t.TaskType == copy {
			task_group.TransferCoordinator.AppendPayload(t.groupID, task_group.DstPathToRefresh(dstActualPath))
		}
		for _, obj := range objs {
			if utils.IsCanceled(t.Ctx()) {
				return nil
			}
			task := &FileTransferTask{
				TaskType: t.TaskType,
				TaskData: TaskData{
					TaskExtension: task.TaskExtension{
						Creator: t.GetCreator(),
						ApiUrl:  t.ApiUrl,
					},
					SrcStorage:    srcStorage,
					DstStorage:    dstStorage,
					SrcActualPath: stdpath.Join(srcActualPath, obj.GetName()),
					DstActualPath: dstActualPath,
					SrcStorageMp:  srcStorage.GetStorage().MountPath,
					DstStorageMp:  dstStorage.GetStorage().MountPath,
				},
				groupID: t.groupID,
			}
			task_group.TransferCoordinator.AddTask(t.groupID, nil)
			if t.TaskType == copy {
				CopyTaskManager.Add(task)
			} else {
				MoveTaskManager.Add(task)
			}
		}
		t.Status = fmt.Sprintf("src object is dir, added all %s tasks of objs", t.TaskType)
		return nil
	}
	return putFileBetween2Storages(t, srcStorage, dstStorage, srcActualPath, dstDirActualPath)
}

func putFileBetween2Storages(tsk *FileTransferTask, srcStorage, dstStorage driver.Driver, srcActualPath, dstDirActualPath string) error {
	srcFile, err := op.Get(tsk.Ctx(), srcStorage, srcActualPath)
	if err != nil {
		return errors.WithMessagef(err, "failed get src [%s] file", srcActualPath)
	}
	tsk.SetTotalBytes(srcFile.GetSize())
	link, _, err := op.Link(tsk.Ctx(), srcStorage, srcActualPath, model.LinkArgs{})
	if err != nil {
		return errors.WithMessagef(err, "failed get [%s] link", srcActualPath)
	}
	// any link provided is seekable
	ss, err := stream.NewSeekableStream(&stream.FileStream{
		Obj: srcFile,
		Ctx: tsk.Ctx(),
	}, link)
	if err != nil {
		_ = link.Close()
		return errors.WithMessagef(err, "failed get [%s] stream", srcActualPath)
	}
	tsk.SetTotalBytes(ss.GetSize())
	return op.Put(tsk.Ctx(), dstStorage, dstDirActualPath, ss, tsk.SetProgress, true)
}

var (
	CopyTaskManager *tache.Manager[*FileTransferTask]
	MoveTaskManager *tache.Manager[*FileTransferTask]
)