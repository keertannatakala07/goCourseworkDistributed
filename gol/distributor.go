package gol

import (
	"encoding/gob"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
	keyPresses <-chan rune
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

func calculateAliveCells(p Params, world [][]byte) []util.Cell {
	cellCoordsSlice := []util.Cell{}

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			if world[row][column] == 255 {
				cellCoordsSlice = append(cellCoordsSlice, util.Cell{X: column, Y: row})
			}
		}
	}

	return cellCoordsSlice
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, c distributorChannels) {
	gob.Register([][]byte{})
	c.ioCommand <- ioInput
	filename := fmt.Sprintf("%dx%d", p.ImageWidth, p.ImageHeight)
	c.ioFilename <- filename

	worldSlice := make([][]byte, p.ImageHeight)
	for i := 0; i < len(worldSlice); i++ {
		worldSlice[i] = make([]byte, p.ImageWidth)
	}

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			worldSlice[row][column] = <-c.ioInput
		}
	}

	turn := 0
	c.events <- StateChange{turn, Executing}

	workerAddresses := []string{"127.0.0.1:8080", "127.0.0.1:8081"}
	totalWorkers := len(workerAddresses)
	chunks := p.ImageHeight / totalWorkers

	var wg sync.WaitGroup
	var mut sync.Mutex
	endResponses := make([]WorkerSliceResponse, totalWorkers)

	var mu sync.Mutex
	paused := false

	done := make(chan struct{})
	pauseTicker := make(chan bool)
	isClosed := false
	shutdown := func() {
		mu.Lock()
		if !isClosed {
			isClosed = true
			close(done)
		}
		mu.Unlock()
	}

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		client, err := rpc.Dial("tcp", workerAddresses[0])
		if err != nil {
			return
		}
		defer client.Close()

		for {
			select {
			case <-done:
				return

			case paused := <-pauseTicker:
				if paused {
					ticker.Stop()
				} else {
					ticker.Reset(2 * time.Second)
				}
			case <-ticker.C:
				var aliveRes Response
				// Call the worker's GetAliveCells method every 2 seconds
				err := client.Call("CalculateTurnService.GetAliveCells", &EmptyArgs{}, &aliveRes)
				if err == nil {
					c.events <- AliveCellsCount{CompletedTurns: aliveRes.CompletedTurns, CellsCount: aliveRes.AliveCells}
				} else {
					fmt.Println("Error fetching alive cells: ", err)
				}
			}
		}
	}()

	go func() {
		client, err := rpc.Dial("tcp", workerAddresses[0])
		if err != nil {
			return
		}
		defer client.Close()
		for {
			select {
			case k := <-c.keyPresses:
				// Forward key presses to the worker process so it owns the pause/quit/save state.
				keyPressReq := KeyPressRequest{Key: k}
				keyPressRes := KeyPressResponse{}
				err := client.Call("CalculateTurnService.HandleKeyPress", keyPressReq, &keyPressRes)
				if err != nil {
					fmt.Println("Error handling key press:", err)
					continue
				}

				switch k {
				case 's':
					if !keyPressRes.SaveRequested {
						continue
					}

					c.ioCommand <- ioOutput

					outputFilename := fmt.Sprintf(
						"%dx%dx%d",
						p.ImageWidth,
						p.ImageHeight,
						keyPressRes.CompletedTurns,
					)

					c.ioFilename <- outputFilename

					for row := 0; row < p.ImageHeight; row++ {
						for column := 0; column < p.ImageWidth; column++ {
							c.ioOutput <- keyPressRes.SaveWorld[row][column]
						}
					}

					c.ioCommand <- ioCheckIdle
					<-c.ioIdle

					c.events <- ImageOutputComplete{
						CompletedTurns: keyPressRes.CompletedTurns,
						Filename:       outputFilename,
					}
				case 'q', 'k':
					c.events <- StateChange{
						CompletedTurns: keyPressRes.CompletedTurns,
						NewState:       Quitting,
					}
					return
				case 'p':
					paused = keyPressRes.Paused
					if paused {
						fmt.Printf("Current turn: %d\n", keyPressRes.CompletedTurns)
						c.events <- StateChange{
							CompletedTurns: keyPressRes.CompletedTurns,
							NewState:       Paused,
						}
						pauseTicker <- paused
					} else {
						fmt.Println("Continuing")
						c.events <- StateChange{
							CompletedTurns: keyPressRes.CompletedTurns,
							NewState:       Executing,
						}
						pauseTicker <- paused
					}
				}
			case <-done:
				return
			}
		}
	}()

	for i := 0; i < totalWorkers; i++ {
		wg.Add(1)
		go func(workerIx int) {
			defer wg.Done()

			startY := workerIx * chunks
			endY := (workerIx + 1) * chunks

			if workerIx == totalWorkers-1 {
				endY = p.ImageHeight
			}

			subSlice := worldSlice[startY:endY]

			client, err := rpc.Dial("tcp", workerAddresses[workerIx])
			if err != nil {
				fmt.Printf("Error connecting")
				return
			}
			defer client.Close()

			var aboveAddress, belowAddress string

			if workerIx > 0 {
				aboveAddress = workerAddresses[workerIx-1]

			}
			if workerIx < totalWorkers-1 {
				belowAddress = workerAddresses[workerIx+1]
			}

			request := WorkerSliceRequest{
				GolSlice:        subSlice,
				Turns:           p.Turns,
				ImageWidth:      p.ImageWidth,
				Height:          p.ImageHeight,
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
	shutdown()

	worldPutTogether := make([][]byte, p.ImageHeight)
	totalAlive := 0

	for workerIx, res := range endResponses {
		startY := workerIx * chunks
		for rIx, row := range res.NewGolSlice {
			worldRow := startY + rIx
			if worldRow < p.ImageHeight {
				worldPutTogether[worldRow] = row
			}
		}
		totalAlive += res.AliveCells
	}

	completedTurns := p.Turns
	aliveCells := calculateAliveCells(p, worldPutTogether)

	c.ioCommand <- ioOutput
	var outputFilename string

	outputFilename = fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, completedTurns)

	c.ioFilename <- outputFilename

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			c.ioOutput <- worldPutTogether[row][column]
		}
	}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{CompletedTurns: completedTurns, Alive: aliveCells}
	c.events <- ImageOutputComplete{CompletedTurns: completedTurns, Filename: outputFilename}
	c.events <- StateChange{CompletedTurns: completedTurns, NewState: Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
