package eth

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/panjf2000/ants/v2"
	"github.com/supermigo/evmscanblock/daemon"
	"github.com/supermigo/evmscanblock/db"
	"github.com/supermigo/xlog"
	"math/big"
	"strconv"
	"sync"
	"time"
)

// BlockListen /*scan the chain block by block*/
type BlockListen struct {
	Log                      chan types.Log //每个eth时间
	EthClient                *ethclient.Client
	EventConsumerMap         map[string]*EventConsumer
	TxMonitors               map[common.Address]ITxMonitor
	errC                     chan error
	blockTime                uint64
	blockOffset              int64
	chainID                  int32
	BlockScanTimeSleep       time.Duration
	syncMutex                sync.RWMutex
	ChannelStringForCustomer chan IEventConsumer
	DeleteStringForCustomer  chan IEventConsumer
}

func NewEthListener(
	ethInfo *db.SyncStatus,
	ethClient *ethclient.Client,
	blockTime uint64,
	blockOffset int64,
	ChainID int32,
	BlockScanTimeSleep time.Duration,
) *BlockListen {
	st := &BlockListen{
		EthClient:                ethClient,
		EventConsumerMap:         make(map[string]*EventConsumer),
		errC:                     make(chan error),
		blockTime:                blockTime,
		blockOffset:              blockOffset,
		chainID:                  ChainID,
		ChannelStringForCustomer: make(chan IEventConsumer, 0),
		DeleteStringForCustomer:  make(chan IEventConsumer, 0),
		BlockScanTimeSleep:       BlockScanTimeSleep,
	}
	return st
}
func (st *BlockListen) ReadConsumer(ctx context.Context) {
	for {
		select {
		case address := <-st.ChannelStringForCustomer:
			st.RegisterConsumer(address)
		case address := <-st.DeleteStringForCustomer:
			st.DeleteConsumer(address)
		case <-ctx.Done():
			return
		}
	}
}
func (s *BlockListen) DeleteConsumer(consumer IEventConsumer) error {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()
	consumerHandler, err := consumer.GetConsumer()
	if err != nil {
		xlog.CError("[eth listener] Unable to get consumer", err.Error())
		return err
	}
	for i := 0; i < len(consumerHandler); i++ {
		xlog.CDebugf("Key for comsumer: %+v", KeyFromBEConsumer(consumerHandler[i].Address.Hex(), consumerHandler[i].Topic.Hex()))
		if _, ok := s.EventConsumerMap[KeyFromBEConsumer(consumerHandler[i].Address.Hex(), consumerHandler[i].Topic.Hex())]; !ok {
			delete(s.EventConsumerMap, KeyFromBEConsumer(consumerHandler[i].Address.Hex(), consumerHandler[i].Topic.Hex()))
		}
	}
	return nil
}

func KeyFromBEConsumer(address string, topic string) string {
	return fmt.Sprintf("%s:%s", address, topic)
}

func (s *BlockListen) RegisterConsumer(consumer IEventConsumer) error {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()
	consumerHandler, err := consumer.GetConsumer()
	if err != nil {
		xlog.CError("[eth listener] Unable to get consumer", err.Error())
		return err
	}
	for i := 0; i < len(consumerHandler); i++ {
		xlog.CDebugf("Key for comsumer: %+v", KeyFromBEConsumer(consumerHandler[i].Address.Hex(), consumerHandler[i].Topic.Hex()))
		if _, ok := s.EventConsumerMap[KeyFromBEConsumer(consumerHandler[i].Address.Hex(), consumerHandler[i].Topic.Hex())]; !ok {
			s.EventConsumerMap[KeyFromBEConsumer(consumerHandler[i].Address.Hex(), consumerHandler[i].Topic.Hex())] = consumerHandler[i]
		}
	}
	return nil
}

func (s *BlockListen) Start(ctx context.Context) {
	daemon.BootstrapDaemons(ctx, s.Scan)
}
func (s *BlockListen) Scan(parentContext context.Context) (daemonfun daemon.Daemon, err error) {
	daemonfun = func() {
		defer func() {
			close(s.DeleteStringForCustomer)
			close(s.ChannelStringForCustomer)
			close(s.errC)
			close(s.Log)
		}()
		for {
			select {
			case <-parentContext.Done():
				xlog.CDebug("[eth listener] Stop listening")
				return
			default:
				sysInfo, err := db.DB.GetSyncStatus(s.chainID)
				if err != nil {
					xlog.CError("[eth_listener] can't get system info", err.Error())
					continue
				}
				header, err := s.EthClient.HeaderByNumber(parentContext, nil)
				if err != nil {
					xlog.CError("[eth_listener] can't get head by number, possibly due to rpc node failure", err.Error())
					continue
				}
				currBlock := header.Number
				scannedBlock := sysInfo.LastEthBlockBigInt()
				if t := big.NewInt(0).Sub(currBlock, scannedBlock); t.Int64() > 10 {
					xlog.CInfo("ChainID "+strconv.Itoa(int(s.chainID))+" 相差块：", t.Int64())
				}
				for begin := scannedBlock; currBlock.Cmp(begin) > 0; begin = begin.Add(begin, big.NewInt(1)) {
					block, err := s.EthClient.BlockByNumber(context.Background(), begin)
					if err != nil {
						xlog.CError(err.Error())
						begin = big.NewInt(0).Sub(begin, big.NewInt(1))
						continue
					}
					var wg sync.WaitGroup
					p, _ := ants.NewPoolWithFunc(20, func(input interface{}) {
						tx := input.(*types.Transaction)
						receipt, err := s.EthClient.TransactionReceipt(context.Background(), tx.Hash())
						if err != nil {
							xlog.CError(err.Error())
						} else {
							for _, vLog := range receipt.Logs {
								consumer, isExisted := s.matchEvent(*vLog)
								if isExisted {
									err := consumer.ParseEvent(*vLog)
									if err != nil {
										xlog.CErrorf("[eth_client] Consume event error")
									}
								}
							}
						}
						wg.Done()
					})
					for key := 0; key < len(block.Transactions()); key++ {
						wg.Add(1)
						tx := block.Transactions()[key]
						_ = p.Invoke(tx)
					}
					wg.Wait()
					p.Release()
					sysInfo.LastEthBlockRecorded = begin.Uint64()
					db.DB.UpdateSyncStatusWithBlockNumber(s.chainID, sysInfo.LastEthBlockRecorded)
					if s.BlockScanTimeSleep != 0 {
						daemon.SleepContext(parentContext, s.BlockScanTimeSleep)
					}
				}

				header2, err := s.EthClient.HeaderByNumber(parentContext, nil)
				if err == nil {
					if diff := big.NewInt(0).Sub(header2.Number, currBlock); diff.Int64() == 0 {
						xlog.CInfo("now is block is equal")
						daemon.SleepContext(parentContext, time.Second*time.Duration(s.blockTime))
					} else {
						xlog.CInfo("ChainID "+strconv.Itoa(int(s.chainID))+" 相差块：", diff.Int64())
					}
				}
			}

		}
	}
	return daemonfun, nil
}

func (s *BlockListen) matchEvent(vLog types.Log) (*EventConsumer, bool) {
	if len(vLog.Topics) == 0 {
		return nil, false
	}
	key := KeyFromBEConsumer(vLog.Address.Hex(), vLog.Topics[0].Hex())
	s.syncMutex.RLock()
	defer s.syncMutex.RUnlock()
	consumer, isExisted := s.EventConsumerMap[key]
	return consumer, isExisted
}

func (s *BlockListen) consumeEvent(vLog types.Log) {
	consumer, isExisted := s.matchEvent(vLog)
	if isExisted {
		err := consumer.ParseEvent(vLog)
		if err != nil {
			xlog.CErrorf("[eth_client] Consume event error")
		}
	}
}
