package eth

import (
	"context"
	"errors"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/supermigo/evmscanblock/daemon"
	"github.com/supermigo/evmscanblock/db"
	"github.com/supermigo/xlog"
	"math/big"
	"strconv"
	"sync"
	"time"
)

type FilterEthListener struct {
	Log              chan types.Log
	EthClient        *ethclient.Client
	EventFilters     []ethereum.FilterQuery
	EventConsumerMap map[string]*EventConsumer
	TxMonitors       map[common.Address]ITxMonitor
	errC             chan error
	blockTime        uint64
	blockOffset      int64
	chainID          int32
}

func NewEthListenerByFilterQuery(
	ethClient *ethclient.Client,
	blockTime uint64,
	blockOffset int64,
	chainID int32,
) *FilterEthListener {
	return &FilterEthListener{
		EthClient:        ethClient,
		EventConsumerMap: make(map[string]*EventConsumer),
		TxMonitors:       make(map[common.Address]ITxMonitor),
		Log:              make(chan types.Log),
		errC:             make(chan error),
		blockTime:        blockTime,
		blockOffset:      blockOffset,
		chainID:          chainID,
	}
}

func (s *FilterEthListener) AddFilterQuery(query ethereum.FilterQuery) {
	s.EventFilters = append(s.EventFilters, query)
}

func (s *FilterEthListener) RegisterConsumer(consumer IEventConsumer) error {
	consumerHandler, err := consumer.GetConsumer()
	if err != nil {
		xlog.CError("[eth listener] Unable to get consumer", err.Error())
		return err
	}
	for i := 0; i < len(consumerHandler); i++ {
		s.EventConsumerMap[KeyFromBEConsumer(consumerHandler[i].Address.Hex(), consumerHandler[i].Topic.Hex())] = consumerHandler[i]
	}

	s.EventFilters = append(s.EventFilters, consumer.GetFilterQuery()...)
	return nil
}

func (s *FilterEthListener) RegisterTxMonitor(monitor ITxMonitor) error {
	if monitor == nil {
		err := errors.New("nil monitor")
		xlog.CError(err.Error(), "[eth listener] Register nil monitor")
		return err
	}
	address := monitor.MonitoredAddress()
	if _, ok := s.TxMonitors[address]; !ok {
		s.TxMonitors[address] = monitor
		return nil
	}
	return errors.New("Monitor for 0x:" + address.Hex() + " already existed")
}

func (s *FilterEthListener) Start(ctx context.Context) {
	daemon.BootstrapDaemons(ctx, s.Handling, s.Scan)
}

func (s *FilterEthListener) Handling(parentContext context.Context) (fn daemon.Daemon, err error) {
	fn = func() {
		for {
			select {
			case err := <-s.errC:
				xlog.CError(err.Error(), "[eth_listener] Ethereum client scan block err")
			case vLog := <-s.Log:
				go func(vLog types.Log) {
					s.consumeEvent(vLog)
				}(vLog)

			case <-parentContext.Done():
				return
			}
		}

	}
	return fn, nil
}

// Scan  offset is the number of block to scan before current block to make sure event is confirmed
func (s *FilterEthListener) Scan(parentContext context.Context) (daemonfn daemon.Daemon, err error) {
	// scanners

	blockTransferScanner := func(from *big.Int, to *big.Int) {
		for i := from; i.Cmp(to) < 1; i = i.Add(i, big.NewInt(1)) {
			currBlock, err := s.EthClient.BlockByNumber(context.Background(), i)
			if err != nil {
				xlog.CError(err.Error(), "[eth_listener]] Ethereum tx scan err")
				s.errC <- err
				continue
			}
			trans := currBlock.Transactions()
			for _, t := range trans {
				if t.To() == nil { // contract deployment, ignore
					continue
				}
				var isContractCall = false
				if len(t.Data()) > 0 {
					continue
				}

				if monitor, ok := s.TxMonitors[*t.To()]; ok && !isContractCall {
					xlog.CInfo("tran to: %+v", *t.To())
					msg, err := t.AsMessage(types.LatestSignerForChainID(t.ChainId()), nil)
					if err != nil {
						xlog.CError(err.Error(), "Unable to convert transaction to message")
						continue
					}
					from := msg.From().Hex()
					to := msg.To().Hex()
					amount := t.Value().String()
					monitor.TxParse(t, from, to, monitor.MonitoredAddress().String(), amount)
				}
			}
		}
	}

	eventScanner := func(query ethereum.FilterQuery, from, to *big.Int) {
		query.FromBlock = from //scannedBlock
		query.ToBlock = to     //currBlock
		// get all event
		events, err := s.EthClient.FilterLogs(context.Background(), query)
		if err != nil {
			xlog.CError(err.Error(), "[eth_listener]] Ethereum filter query err")
			s.errC <- err
		}
		for _, event := range events {
			s.Log <- event
		}
	}

	// main scanning daemon
	daemonfn = func() {
		for {
			select {
			case <-parentContext.Done():
				return
			default:
				sysInfo, err := db.DB.GetSyncStatus(s.chainID)
				if err != nil {
					xlog.CError(err.Error(), "[eth_listener] can't get system info")
					continue
				}

				header, err := s.EthClient.HeaderByNumber(parentContext, nil)
				if err != nil {
					xlog.CError(err.Error(), "[eth_listener] can't get head by number, possibly due to rpc node failure")
					continue
				}
				currBlock := header.Number

				var scannedBlock *big.Int
				if sysInfo.LastEthBlockRecorded <= 0 {
					// set first block
					scannedBlock = big.NewInt(s.blockOffset)
				} else {
					scannedBlock = big.NewInt(int64(sysInfo.LastEthBlockRecorded))
				}

				// scanned a offset - 1 block before to sure event confirmed
				// scannedBlock = scannedBlock.Sub(scannedBlock, big.NewInt(s.blockOffset-1))

				// if last scanned block is more than $BIGNUM blocks away just scan last $BIGNUM blocks
				diff := big.NewInt(0).Sub(currBlock, scannedBlock)
				if diff.Cmp(big.NewInt(600000)) > 0 {
					diff = big.NewInt(600000)
					scannedBlock = scannedBlock.Sub(currBlock, diff)
				} else {
					xlog.CInfo("ChainID "+strconv.Itoa(int(s.chainID))+" 相差块：", diff.Int64())
				}

				if diff.Cmp(big.NewInt(100)) > 0 {
					// scan in 100-chunk
					for begin := scannedBlock; currBlock.Cmp(begin) > 0; begin = begin.Add(begin, big.NewInt(100)) {
						limit := big.NewInt(99)
						curDiff := big.NewInt(0)
						if curDiff.Sub(currBlock, begin).Cmp(limit) < 0 {
							limit = currBlock.Sub(currBlock, begin)
						}
						until := big.NewInt(0)
						until = until.Add(begin, limit)
						// tx scan
						wg := sync.WaitGroup{}
						if len(s.TxMonitors) > 0 {
							wg.Add(1)
							go func(from *big.Int, to *big.Int) {
								blockTransferScanner(from, to)
								wg.Done()
							}(begin, until)
						}
						xlog.CInfo(">>>>", len(s.EventFilters))
						// events scan
						for _, query := range s.EventFilters {
							wg.Add(1)
							go func(query ethereum.FilterQuery) {
								eventScanner(query, begin, until)
								xlog.CInfo(">>>>123123123")
								wg.Done()
							}(query)
						}
						wg.Wait()
						xlog.CInfo(">>>>", len(s.EventFilters), "ok")
						// update last scan block
						sysInfo.LastEthBlockRecorded = until.Uint64()
						db.DB.UpdateSyncStatusWithBlockNumber(s.chainID, sysInfo.LastEthBlockRecorded)
					}
				} else {
					if scannedBlock.Int64() == currBlock.Int64() {
						daemon.SleepContext(parentContext, time.Second*time.Duration(s.blockTime))
						continue
					}
					xlog.CInfof("[eth_listener] scan from block %s to %s", scannedBlock.String(), currBlock.String())
					scannedBlock = scannedBlock.Add(scannedBlock, big.NewInt(1))
					// tx scan
					wg := sync.WaitGroup{}
					if len(s.TxMonitors) > 0 {
						wg.Add(1)
						go func(from *big.Int, to *big.Int) {
							blockTransferScanner(from, to)
							wg.Done()
						}(scannedBlock, currBlock)
					}

					// events scan
					for _, query := range s.EventFilters {
						wg.Add(1)
						go func(query ethereum.FilterQuery) {
							eventScanner(query, scannedBlock, currBlock)
							wg.Done()
						}(query)
					}
					wg.Wait()
					sysInfo.LastEthBlockRecorded = currBlock.Uint64()
					db.DB.UpdateSyncStatusWithBlockNumber(s.chainID, sysInfo.LastEthBlockRecorded)
				}

				// TODO: either push this to delay message queue to run OR just sleep
				daemon.SleepContext(parentContext, time.Second*time.Duration(s.blockTime))
			}
		}
	}
	return daemonfn, nil
}

func (s *FilterEthListener) matchEvent(vLog types.Log) (*EventConsumer, bool) {
	key := KeyFromBEConsumer(vLog.Address.Hex(), vLog.Topics[0].Hex())
	consumer, isExisted := s.EventConsumerMap[key]
	return consumer, isExisted
}

func (s *FilterEthListener) consumeEvent(vLog types.Log) {
	consumer, isExisted := s.matchEvent(vLog)
	if isExisted {
		err := consumer.ParseEvent(vLog)
		if err != nil {
			xlog.CError(err.Error(), "[eth_client] Consume event error")
		}
	}
}
