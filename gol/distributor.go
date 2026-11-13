package gol

import (
	"fmt"
	"net/rpc"
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

type EmptyArgs struct{}

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

	completedTurns := 0

	client, err := rpc.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	defer client.Close()

	req := Request{
		WorldSlice:  worldSlice,
		Turns:       p.Turns,
		ImageHeight: p.ImageHeight,
		ImageWidth:  p.ImageWidth,
	}

	var res Response

	done := make(chan bool)

	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
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

	fmt.Printf("calling worker\n")
	err = client.Call("CalculateTurnService.CalculateTurns", req, &res)

	close(done)

	if err != nil {
		fmt.Println("Error: ", err)
	}

	worldSlice = res.NewWorldSlice
	completedTurns = res.CompletedTurns

	aliveCells := calculateAliveCells(p, worldSlice)

	c.ioCommand <- ioOutput
	var outputFilename string

	outputFilename = fmt.Sprintf("%dx%dx%d", p.ImageWidth, p.ImageHeight, completedTurns)

	c.ioFilename <- outputFilename

	for row := 0; row < p.ImageHeight; row++ {
		for column := 0; column < p.ImageWidth; column++ {
			c.ioOutput <- worldSlice[row][column]
		}
	}

	// Make sure that the Io has finished any output before exiting.
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	// TODO: Report the final state using FinalTurnCompleteEvent.
	c.events <- FinalTurnComplete{CompletedTurns: completedTurns, Alive: aliveCells}
	c.events <- ImageOutputComplete{CompletedTurns: completedTurns, Filename: outputFilename}
	c.events <- StateChange{turn, Quitting}

	// Close the channel to stop the SDL goroutine gracefully. Removing may cause deadlock.
	close(c.events)
}
