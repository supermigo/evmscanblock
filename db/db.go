package db

import (
	"github.com/supermigo/xlog"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	gormlog "gorm.io/gorm/logger"
	"math/big"

	"time"
)

type ScanDB struct {
	Instance *gorm.DB
}

var (
	DB ScanDB
)

func Init(str string, configValue gormlog.Config) {

	dbFileName := str
	db, err := gorm.Open(sqlite.Open(dbFileName), &gorm.Config{
		Logger: xlog.NewGormLogger(configValue),
	})
	if err != nil {
		panic("can't save sqlite3 db")
		return
	}
	sqlDB, err := db.DB()
	db.Exec("PRAGMA journal_mode=WAL")
	sqlDB.SetConnMaxLifetime(time.Hour * 1)
	sqlDB.SetMaxOpenConns(3)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxIdleTime(time.Minute * 10)
	db.AutoMigrate(&SyncStatus{})
	DB.Instance = db
	return

}

// Maintains sync information
type SyncStatus struct {
	gorm.Model
	ChainID int32 `json:"chainID"`
	// Last eth block seen by the syncer is persisted here so that we can resume sync from it
	LastEthBlockRecorded uint64 `json:"lastEthBlockRecorded"` //更新的最后一个块的数据

	// Last batch index is recorded for this field
	LastBatchRecorded uint64 `json:"lastBatchRecorded"`
}

func (*SyncStatus) TableName() string {
	return "sync_statuses"
}
func (ss *SyncStatus) LastEthBlockBigInt() *big.Int {
	n := new(big.Int)
	return n.SetUint64(ss.LastEthBlockRecorded)
}

func (db *ScanDB) UpdateSyncStatusWithBatchNumber(ChainID int32, batchIndex uint64) error {
	var updatedSyncStatus SyncStatus
	updatedSyncStatus.LastBatchRecorded = batchIndex
	if err := db.Instance.Table("sync_statuses").Assign(SyncStatus{LastBatchRecorded: batchIndex}).Where("chain_id=?", ChainID).FirstOrCreate(&updatedSyncStatus).Error; err != nil {
		return err
	}
	return nil
}
func (db *ScanDB) UpdateSyncStatusWithBlockNumber(ChainID int32, blkNum uint64) error {
	var updatedSyncStatus SyncStatus
	updatedSyncStatus.LastEthBlockRecorded = blkNum
	if err := db.Instance.Table("sync_statuses").Assign(SyncStatus{LastEthBlockRecorded: blkNum, ChainID: ChainID}).Where("chain_id=?", ChainID).FirstOrCreate(&updatedSyncStatus).Error; err != nil {
		return err
	}
	return nil
}
func (db *ScanDB) ResetUpdateSyncStatusWithBlockNumber(ChainID int32, blkNum uint64) error {
	var updatedSyncStatus SyncStatus
	updatedSyncStatus.LastEthBlockRecorded = blkNum
	if err := db.Instance.Table("sync_statuses").Assign(SyncStatus{LastEthBlockRecorded: blkNum, ChainID: ChainID}).Where("chain_id=?", ChainID).Updates(&updatedSyncStatus).Error; err != nil {
		return err
	}
	return nil
}
func (db *ScanDB) GetSyncStatus(ChainID int32) (status SyncStatus, err error) {
	if err := db.Instance.Where("chain_id=?", ChainID).First(&status).Error; err != nil {
		return status, err
	}
	return status, nil
}
