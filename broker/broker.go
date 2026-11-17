package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"
)

type BrokerRequest struct {
	World       [][]byte
	Turns       int
	ImageHeight int
	ImageWidth  int
}
type BrokerResponse struct {
	NewWorld       [][]byte
	CompletedTurns int
	AliveCells     int
}

type Request struct {
	WorldSlice  [][]byte
	Turns       int
	ImageHeight int
	ImageWidth  int
}

type Response struct {
	NewWorldSlice  [][]byte
	CompletedTurns int
	AliveCells     int
}

type KeyPressRequest struct {
	Key rune
}

type KeyPressResponse struct {
	Paused         bool
	SaveRequested  bool
	SaveWorld      [][]byte
	CompletedTurns int
}

type EmptyArgs struct{}

type WorkerSliceRequest struct {
	GolSlice        [][]byte
	Turns           int
	ImageWidth      int
	Height          int
	WorkerIndex     int
	NumberOfWorkers int
	AboveWorker     string
	BelowWorker     string
}

type WorkerSliceResponse struct {
	NewGolSlice    [][]byte
	CompletedTurns int
	AliveCells     int
}

type BrokerService struct {
	workerAddresses []string
	imageHeight     int
	imageWidth      int
	listener        net.Listener
}

func (b *BrokerService) StartState(req Request, res *Response) error {
	b.imageHeight = req.ImageHeight
	b.imageWidth = req.ImageWidth

	totalWorkers := len(b.workerAddresses)
	chunks := req.ImageHeight / totalWorkers

	var wg sync.WaitGroup
	var mut sync.Mutex
	endResponses := make([]WorkerSliceResponse, totalWorkers)

	for i := 0; i < totalWorkers; i++ {
		wg.Add(1)
		go func(workerIx int) {
			defer wg.Done()

			startY := workerIx * chunks
			endY := (workerIx + 1) * chunks

			if workerIx == totalWorkers-1 {
				endY = req.ImageHeight
			}

			subSlice := req.WorldSlice[startY:endY]

			client, err := rpc.Dial("tcp", b.workerAddresses[workerIx])
			if err != nil {
				fmt.Printf("Error connecting")
				return
			}
			defer client.Close()

			var aboveAddress, belowAddress string

			if workerIx > 0 {
				aboveAddress = b.workerAddresses[workerIx-1]

			}
			if workerIx < totalWorkers-1 {
				belowAddress = b.workerAddresses[workerIx+1]
			}

			request := WorkerSliceRequest{
				GolSlice:        subSlice,
				Turns:           req.Turns,
				ImageWidth:      req.ImageWidth,
				Height:          req.ImageHeight,
				WorkerIndex:     workerIx,
				NumberOfWorkers: totalWorkers,
				AboveWorker:     aboveAddress,
				BelowWorker:     belowAddress,
			}

			var res WorkerSliceResponse
			client.Call("CalculateTurnService.CalculateTurns", request, &res)

			mut.Lock()
			endResponses[workerIx] = res
			mut.Unlock()
		}(i)

	}

	wg.Wait()

	worldPutTogether := make([][]byte, req.ImageHeight)
	totalAlive := 0

	for workerIx, res := range endResponses {
		startY := workerIx * chunks
		for rIx, row := range res.NewGolSlice {
			worldRow := startY + rIx
			if worldRow < req.ImageHeight {
				worldPutTogether[worldRow] = row
			}
		}
		totalAlive += res.AliveCells
	}

	res.NewWorldSlice = worldPutTogether
	res.CompletedTurns = endResponses[0].CompletedTurns
	res.AliveCells = totalAlive

	return nil
}

func (b *BrokerService) GetAliveCells(args *EmptyArgs, res *Response) error {
	totalAlive := 0
	completedTurns := 0

	for i, address := range b.workerAddresses {
		client, err := rpc.Dial("tcp", address)

		if err != nil {
			continue
		}

		var workerRes Response
		err = client.Call("CalculateTurnService.GetAliveCells", &EmptyArgs{}, &workerRes)
		client.Close()
		if err == nil {
			totalAlive += workerRes.AliveCells
			if i == 0 {
				completedTurns = workerRes.CompletedTurns
			}
		}
	}

	res.CompletedTurns = completedTurns
	res.AliveCells = totalAlive
	return nil
}

func (b *BrokerService) HandleKeyPress(req KeyPressRequest, res *KeyPressResponse) error {
	totalWorkers := len(b.workerAddresses)
	chunks := b.imageHeight / totalWorkers
	workerResponses := make([]KeyPressResponse, totalWorkers)

	var wg sync.WaitGroup
	var mut sync.Mutex

	for i := 0; i < totalWorkers; i++ {
		wg.Add(1)
		go func(workerIx int) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", b.workerAddresses[workerIx])
			if err != nil {
				return
			}
			defer client.Close()

			var kRes KeyPressResponse
			err = client.Call("CalculateTurnService.HandleKeyPress", req, &kRes)
			if err == nil {
				mut.Lock()
				workerResponses[workerIx] = kRes
				mut.Unlock()
			}
		}(i)
	}
	wg.Wait()

	res.Paused = workerResponses[0].Paused
	res.CompletedTurns = workerResponses[0].CompletedTurns
	res.SaveRequested = workerResponses[0].SaveRequested

	if res.SaveRequested {
		savePutTogether := make([][]byte, b.imageHeight)
		for workerIx, kRes := range workerResponses {
			startY := workerIx * chunks
			for rIx, row := range kRes.SaveWorld {
				worldRow := startY + rIx
				if worldRow < b.imageHeight {
					savePutTogether[worldRow] = row
				}
			}
		}
		res.SaveWorld = savePutTogether
	}

	if req.Key == 'k' {
		go func() {
			time.Sleep(100 * time.Millisecond)
			if b.listener != nil {
				b.listener.Close()
			}
		}()
	}

	return nil
}

func main() {
	gob.Register([][]byte{})
	port := flag.String("port", "8080", "Broker listens on")
	workers := flag.String("workers", "127.0.0.1:8081,127.0.0.1:8082", "worker addresses")
	flag.Parse()

	service := &BrokerService{
		workerAddresses: strings.Split(*workers, ","),
	}

	err := rpc.Register(service)
	if err != nil {
		fmt.Println("Failed to register RPC: ", err)
		return
	}

	address := ":" + *port
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Failed to listen: ", err)
		return
	}
	defer listener.Close()

	service.listener = listener

	fmt.Printf("Broker listening on %s...\n", address)
	rpc.Accept(listener)
}
