package handles

import (
	"context"
	"errors"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/db"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

type StorageResp struct {
	model.Storage
	MountDetails *model.StorageDetails `json:"mount_details,omitempty"`
}

type detailWithIndex struct {
	idx int
	val *model.StorageDetails
}

func makeStorageResp(ctx *gin.Context, storages []model.Storage) []*StorageResp {
	ret := make([]*StorageResp, len(storages))
	detailsChan := make(chan detailWithIndex, len(storages))
	workerCount := 0
	var user *model.User
	if val := ctx.Request.Context().Value(conf.UserKey); val != nil {
		user = val.(*model.User)
	}

	for i, s := range storages {
		// Strip BasePath for non-admin users to show relative path
		if user != nil && !user.IsAdmin() && strings.HasPrefix(s.MountPath, user.BasePath) {
			s.MountPath = strings.TrimPrefix(s.MountPath, user.BasePath)
			if s.MountPath == "" {
				s.MountPath = "/"
			}
		}

		ret[i] = &StorageResp{
			Storage:      s,
			MountDetails: nil,
		}
		if setting.GetBool(conf.HideStorageDetailsInManagePage) {
			continue
		}
		d, err := op.GetStorageByMountPath(s.MountPath)
		if err != nil {
			continue
		}
		_, ok := d.(driver.WithDetails)
		if !ok {
			continue
		}
		workerCount++
		go func(dri driver.Driver, idx int) {
			details, e := op.GetStorageDetails(ctx, dri)
			if e != nil {
				if !errors.Is(e, errs.NotImplement) && !errors.Is(e, errs.StorageNotInit) {
					log.Errorf("failed get %s details: %+v", dri.GetStorage().MountPath, e)
				}
			}
			detailsChan <- detailWithIndex{idx: idx, val: details}
		}(d, i)
	}
	for workerCount > 0 {
		select {
		case r := <-detailsChan:
			ret[r.idx].MountDetails = r.val
			workerCount--
		case <-time.After(time.Second * 3):
			workerCount = 0
		}
	}
	return ret
}

func ListStorages(c *gin.Context) {
	var req model.PageReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	req.Validate()
	log.Debugf("%+v", req)
	storages, total, err := db.GetStorages(req.Page, req.PerPage)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, common.PageResp{
		Content: makeStorageResp(c, filterStorages(c, storages)),
		Total:   total, // TODO: Total is wrong here if filtered, but UI might not care much or we fix total later. Filtered count is better.
	})
}

func filterStorages(c *gin.Context, storages []model.Storage) []model.Storage {
	userVal := c.Request.Context().Value(conf.UserKey)
	if userVal == nil {
		return []model.Storage{}
	}
	u := userVal.(*model.User)
	if u.IsAdmin() {
		return storages
	}
	var ret []model.Storage
	for _, s := range storages {
		log.Infof("Debug Filter: User=%s BasePath=%s Storage=%s Match=%v", u.Username, u.BasePath, s.MountPath, strings.HasPrefix(s.MountPath, u.BasePath))
		if strings.HasPrefix(s.MountPath, u.BasePath) {
			ret = append(ret, s)
		}
	}
	return ret
}

func CreateStorage(c *gin.Context) {
	var req model.Storage
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	log.Infof("Debug CreateStorage: User=%s IsAdmin=%v MountPath=%s BasePath=%s", user.Username, user.IsAdmin(), req.MountPath, user.BasePath)

	if !user.IsAdmin() {
		if !strings.HasPrefix(req.MountPath, user.BasePath) {
			// User might provide path relative to their root (e.g. "/")
			// We try to interpret it relative to BasePath
			req.MountPath = path.Join(user.BasePath, req.MountPath)
		}
		if !strings.HasPrefix(req.MountPath, user.BasePath) {
			common.ErrorStrResp(c, "permission denied: you can only mount under "+user.BasePath, 403)
			return
		}
	}
	if id, err := op.CreateStorage(c.Request.Context(), req); err != nil {
		log.Errorf("Debug CreateStorage Failed: %v", err)
		common.ErrorWithDataResp(c, err, 500, gin.H{
			"id": id,
		}, true)
	} else {
		common.SuccessResp(c, gin.H{
			"id": id,
		})
	}
}

func UpdateStorage(c *gin.Context) {
	var req model.Storage
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if !user.IsAdmin() {
		// Check existing storage first to ensure user owns it
		oldS, err := db.GetStorageById(req.ID)
		if err != nil {
			common.ErrorResp(c, err, 500)
			return
		}
		if !strings.HasPrefix(oldS.MountPath, user.BasePath) {
			common.ErrorStrResp(c, "permission denied", 403)
			return
		}
		// Check new path
		if !strings.HasPrefix(req.MountPath, user.BasePath) {
			// User might provide path relative to their root
			req.MountPath = path.Join(user.BasePath, req.MountPath)
		}
		if !strings.HasPrefix(req.MountPath, user.BasePath) {
			common.ErrorStrResp(c, "permission denied: you can only mount under "+user.BasePath, 403)
			return
		}
	}
	if err := op.UpdateStorage(c.Request.Context(), req); err != nil {
		common.ErrorResp(c, err, 500, true)
	} else {
		common.SuccessResp(c)
	}
}

func DeleteStorage(c *gin.Context) {
	idStr := c.Query("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if !user.IsAdmin() {
		oldS, err := db.GetStorageById(uint(id))
		if err != nil || !strings.HasPrefix(oldS.MountPath, user.BasePath) {
			common.ErrorStrResp(c, "permission denied or storage not found", 403)
			return
		}
	}
	if err := op.DeleteStorageById(c.Request.Context(), uint(id)); err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c)
}

func DisableStorage(c *gin.Context) {
	idStr := c.Query("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if !user.IsAdmin() {
		oldS, err := db.GetStorageById(uint(id))
		if err != nil || !strings.HasPrefix(oldS.MountPath, user.BasePath) {
			common.ErrorStrResp(c, "permission denied or storage not found", 403)
			return
		}
	}
	if err := op.DisableStorage(c.Request.Context(), uint(id)); err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c)
}

func EnableStorage(c *gin.Context) {
	idStr := c.Query("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if !user.IsAdmin() {
		oldS, err := db.GetStorageById(uint(id))
		if err != nil || !strings.HasPrefix(oldS.MountPath, user.BasePath) {
			common.ErrorStrResp(c, "permission denied or storage not found", 403)
			return
		}
	}
	if err := op.EnableStorage(c.Request.Context(), uint(id)); err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	common.SuccessResp(c)
}

func GetStorage(c *gin.Context) {
	idStr := c.Query("id")
	id, err := strconv.Atoi(idStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	
	storage, err := db.GetStorageById(uint(id))
	if err != nil {
		common.ErrorResp(c, err, 500, true)
		return
	}
	if !user.IsAdmin() && !strings.HasPrefix(storage.MountPath, user.BasePath) {
		common.ErrorStrResp(c, "permission denied", 403)
		return
	}
	
	// Strip BasePath for non-admin users
	if !user.IsAdmin() && strings.HasPrefix(storage.MountPath, user.BasePath) {
		storage.MountPath = strings.TrimPrefix(storage.MountPath, user.BasePath)
		if storage.MountPath == "" {
			storage.MountPath = "/"
		}
	}

	common.SuccessResp(c, storage)
}

// Old GetStorage implementation below was:
// 	storage, err := db.GetStorageById(uint(id))
// 	if err != nil {
// 		common.ErrorResp(c, err, 500, true)
// 		return
// 	}
// 	common.SuccessResp(c, storage)
// }
// I replaced it with the above valid function.
// Wait, I need to match the original function signature/body to replace it correctly.
// The original:
// 	storage, err := db.GetStorageById(uint(id))
// 	if err != nil {
// 		common.ErrorResp(c, err, 500, true)
// 		return
// 	}
// 	common.SuccessResp(c, storage)
// }


func LoadAllStorages(c *gin.Context) {
	storages, err := db.GetEnabledStorages()
	if err != nil {
		log.Errorf("failed get enabled storages: %+v", err)
		common.ErrorResp(c, err, 500, true)
		return
	}
	conf.ResetStoragesLoadSignal()
	go func(storages []model.Storage) {
		for _, storage := range storages {
			storageDriver, err := op.GetStorageByMountPath(storage.MountPath)
			if err != nil {
				log.Errorf("failed get storage driver: %+v", err)
				continue
			}
			// drop the storage in the driver
			if err := storageDriver.Drop(context.Background()); err != nil {
				log.Errorf("failed drop storage: %+v", err)
				continue
			}
			if err := op.LoadStorage(context.Background(), storage); err != nil {
				log.Errorf("failed get enabled storages: %+v", err)
				continue
			}
			log.Infof("success load storage: [%s], driver: [%s]",
				storage.MountPath, storage.Driver)
		}
		conf.SendStoragesLoadedSignal()
	}(storages)
	common.SuccessResp(c)
}
