package main

// local packages
import "github.com/in3rsha/bitcoin-utxo-dump/bitcoin/btcleveldb" // chainstate leveldb decoding functions
import "github.com/in3rsha/bitcoin-utxo-dump/bitcoin/keys"   // bitcoin addresses
import "github.com/in3rsha/bitcoin-utxo-dump/bitcoin/bech32" // segwit bitcoin addresses

import "github.com/syndtr/goleveldb/leveldb" // go get github.com/syndtr/goleveldb/leveldb
import "github.com/syndtr/goleveldb/leveldb/opt" // set no compression when opening leveldb
import "flag"		 // command line arguments
import "fmt"
import "os"		   // open file for writing
import "os/exec"	  // execute shell command (check bitcoin isn't running)
import "os/signal"	// catch interrupt signals CTRL-C to close db connection safely
import "syscall"	  // catch kill commands too
import "bufio"		// bulk writing to file
import "encoding/hex" // convert byte slice to hexadecimal
import "strings"	  // parsing flags from command line
import "runtime"	  // Check OS type for file-handler limitations
import "sync"		 // For parallel processing
import "strconv"	  // For string conversion

// UTXOJob represents a single UTXO to process
type UTXOJob struct {
	Key   []byte
	Value []byte
	Count int
}

// UTXOResult represents the processed UTXO result
type UTXOResult struct {
	CSVLine   string
	Count	 int
	Amount	int64
	ScriptType string
	Error	 error
}

// BatchWriter for efficient file writing
type BatchWriter struct {
	writer *bufio.Writer
	buffer []string
	mu	 sync.Mutex
	size   int
}

// NewBatchWriter creates a new batch writer
func NewBatchWriter(writer *bufio.Writer, batchSize int) *BatchWriter {
	return &BatchWriter{
		writer: writer,
		buffer: make([]string, 0, batchSize),
		size:   batchSize,
	}
}

// Write adds a line to the buffer and flushes if full
func (bw *BatchWriter) Write(line string) error {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	
	bw.buffer = append(bw.buffer, line)
	if len(bw.buffer) >= bw.size {
		return bw.Flush()
	}
	return nil
}

// Flush writes all buffered lines to file
func (bw *BatchWriter) Flush() error {
	for _, line := range bw.buffer {
		if _, err := bw.writer.WriteString(line + "\n"); err != nil {
			return err
		}
	}
	bw.buffer = bw.buffer[:0] // Clear buffer
	return bw.writer.Flush()
}

func main() {

	// Version
	const Version = "1.0.2"
	
	// Set default chainstate LevelDB and output file
	defaultfolder := fmt.Sprintf("%s/.bitcoin/chainstate/", os.Getenv("HOME")) // %s = string
	defaultfile := "utxodump.csv"
	
	// Command Line Options (Flags)
	chainstate := flag.String("db", defaultfolder, "Location of bitcoin chainstate db.") // chainstate folder
	file := flag.String("o", defaultfile, "Name of file to dump utxo list to.") // output file
	fields := flag.String("f", "count,txid,vout,amount,type,address", "Fields to include in output. [count,txid,vout,height,amount,coinbase,nsize,script,type,address]")
	testnetflag := flag.Bool("testnet", false, "Is the chainstate leveldb for testnet?") // true/false
	verbose := flag.Bool("v", false, "Print utxos as we process them (will be about 3 times slower with this though).")
	version := flag.Bool("version", false, "Print version.")
	p2pkaddresses := flag.Bool("p2pkaddresses", false, "Convert public keys in P2PK locking scripts to addresses also.") // true/false
	nowarnings := flag.Bool("nowarnings", false, "Ignore warnings if bitcoind is running in the background.") // true/false
	quiet := flag.Bool("quiet", false, "Do not display any progress or results.") // true/false
	workers := flag.Int("workers", 0, "Number of worker goroutines (0 = auto-detect)") // Parallel workers
	flag.Parse() // execute command line parsing for all declared flags

	// Check bitcoin isn't running first
	if ! *nowarnings {
		cmd := exec.Command("bitcoin-cli", "getnetworkinfo")
		_, err := cmd.Output()
		if err == nil {
			fmt.Println("Bitcoin is running. You should shut it down with `bitcoin-cli stop` first. We don't want to access the chainstate LevelDB while Bitcoin is running.")
			fmt.Println("Note: If you do stop bitcoind, make sure that it won't auto-restart (e.g. if it's running as a systemd service).")
			
			// Ask if you want to continue anyway (e.g. if you've copied the chainstate to a new location and bitcoin is still running)
			reader := bufio.NewReader(os.Stdin)
			fmt.Printf("%s [y/n] (default n): ", "Do you wish to continue anyway?")
			response, _ := reader.ReadString('\n')
			response = strings.ToLower(strings.TrimSpace(response))

			if response != "y" && response != "yes" {
				return
			}
		}
	}
	
	// Check if OS type is Mac OS, then increase ulimit -n to 4096 filehandler during runtime and reset to 1024 at the end
	// Mac OS standard is 1024
	// Linux standard is already 4096 which is also "max" for more edit etc/security/limits.conf
	if runtime.GOOS == "darwin" {
		cmd2 := exec.Command("ulimit", "-n", "4096")
		if !*quiet {
			fmt.Println("setting ulimit 4096")
		}
		_, err := cmd2.Output()
		if err != nil {
			fmt.Printf("setting new ulimit failed with %s\n", err)
		}
		defer exec.Command("ulimit", "-n", "1024")
	}

	// Show Version
	if *version {
	  fmt.Println(Version)
	  os.Exit(0)
	}

	// Mainnet or Testnet (for encoding addresses correctly)
	testnet := false
	if *testnetflag == true { // check testnet flag
		testnet = true
	} else { // only check the chainstate path if testnet flag has not been explicitly set to true
		if strings.Contains(*chainstate, "testnet") { // check the chainstate path
			testnet = true
		}
	}

	// Check chainstate LevelDB folder exists
	if _, err := os.Stat(*chainstate); os.IsNotExist(err) {
		fmt.Println("Couldn't find", *chainstate)
		return
	}

	// Select bitcoin chainstate leveldb folder
	// open leveldb without compression to avoid corrupting the database for bitcoin
	opts := &opt.Options{
		Compression: opt.NoCompression,
	}
	// https://bitcoin.stackexchange.com/questions/52257/chainstate-leveldb-corruption-after-reading-from-the-database
	// https://github.com/syndtr/goleveldb/issues/61
	// https://godoc.org/github.com/syndtr/goleveldb/leveldb/opt

	db, err := leveldb.OpenFile(*chainstate, opts) // You have got to dereference the pointer to get the actual value
	if err != nil {
		fmt.Println("Couldn't open LevelDB.")
		fmt.Println(err)
		return
	}
	defer db.Close()

	// Create a map of selected fields
	fieldsAllowed := []string{"count", "txid", "vout", "height", "coinbase", "amount", "nsize", "script", "type", "address"}
	fieldsSelected := map[string]bool{"count":false, "txid":false, "vout":false, "height":false, "coinbase":false, "amount":false, "nsize":false, "script":false, "type":false, "address":false}

	// Check that all the given fields are included in the fieldsAllowed array
	for _, v := range strings.Split(*fields, ",") {
		exists := false
		for _, w := range fieldsAllowed {
			if v == w { // check each field against every element in the fieldsAllowed array
				exists = true
			}
		}
		if exists == false {
			fmt.Printf("'%s' is not a field you can use for the output.\n", v)
			fieldsList := ""
			for _, v := range fieldsAllowed {
				fieldsList += v
				fieldsList += ","
			}
			fieldsList = fieldsList[:len(fieldsList)-1] // remove trailing comma
			fmt.Printf("Choose from the following: %s\n", fieldsList)
			return
		}
		// Set field in fieldsSelected map - helps to determine what and what not to calculate later on (to speed processing up)
		if exists == true {
			fieldsSelected[v] = true
		}
	}

	// Open file to write results to.
	f, err := os.Create(*file) // os.OpenFile("filename.txt", os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if ! *quiet {
		fmt.Printf("Processing %s and writing results to %s\n", *chainstate, *file)
	}

	// Create file buffer to speed up writing to the file.
	writer := bufio.NewWriter(f)
	defer writer.Flush() // Flush the bufio buffer to the file before this script ends

	// Create batch writer for efficient file writing
	batchWriter := NewBatchWriter(writer, 1000)
	defer batchWriter.Flush()
	
	// CSV Headers
	csvheader := ""
	for _, v := range strings.Split(*fields, ",") {
		csvheader += v
		csvheader += ","
	} // count,txid,vout,
	csvheader = csvheader[:len(csvheader)-1] // remove trailing ,
	if ! *quiet {
		fmt.Println(csvheader)
	}
	fmt.Fprintln(writer, csvheader) // write to file

	// Stats - keep track of interesting stats as we read through leveldb.
	var totalAmount int64 = 0 // total amount of satoshis
	scriptTypeCount := map[string]int{"p2pk":0, "p2pkh":0, "p2sh":0, "p2ms":0, "p2wpkh":0, "p2wsh":0, "p2tr": 0, "non-standard": 0} // count each script type

	// Declare obfuscateKey (a byte slice)
	var obfuscateKey []byte // obfuscateKey := make([]byte, 0)

	// Iterate over LevelDB keys to find obfuscateKey first
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		prefix := key[0]
		
		// obfuscateKey (first key)
		if (prefix == 14) { // 14 = obfuscateKey
			obfuscateKey = make([]byte, len(value))
			copy(obfuscateKey, value)
			break
		}
	}
	iter.Release()

	// Determine number of workers
	numWorkers := *workers
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU() * 3 / 4
		if numWorkers < 1 {
			numWorkers = 1
		}
	}

	if !*quiet {
		fmt.Printf("Using %d workers for parallel processing\n", numWorkers)
	}

	// Process UTXOs in parallel
	results, totalUTXOs := processUTXOsParallel(db, obfuscateKey, fieldsSelected, testnet, *p2pkaddresses, numWorkers, *fields)

	// Catch signals that interrupt the script so that we can close the database safely (hopefully not corrupting it)
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() { // goroutine
		<-c // receive from channel
		if ! *quiet {
			fmt.Println("Interrupt signal caught. Shutting down gracefully.")
		}
		db.Close()	 // close database
		batchWriter.Flush() // flush batch writer
		writer.Flush() // flush bufio to the file
		f.Close()	  // close file
		os.Exit(0)	 // exit
	}()

	// Process results
	i := 0
	statsMutex := sync.Mutex{}
	
	for result := range results {
		if result.Error != nil {
			if !*quiet {
				fmt.Printf("Error processing UTXO %d: %v\n", result.Count, result.Error)
			}
			continue
		}
		
		// Write to file using batch writer
		if err := batchWriter.Write(result.CSVLine); err != nil {
			fmt.Printf("Error writing to file: %v\n", err)
			return
		}
		
		// Update stats (thread-safe)
		statsMutex.Lock()
		totalAmount += result.Amount
		scriptTypeCount[result.ScriptType]++
		statsMutex.Unlock()
		
		// Progress reporting
		if !*quiet {
			if *verbose {
				fmt.Println(result.CSVLine)
			} else if i > 0 && i%100000 == 0 {
				percentage := float64(i) / float64(totalUTXOs) * 100
				fmt.Printf("%d/%d utxos processed (%.1f%%)\n", i, totalUTXOs, percentage)
			}
		}
		i++
	}

	// Final Progress Report
	// ---------------------
	if ! *quiet {
		fmt.Println()
		fmt.Printf("Total UTXOs: %d\n", i)

		// Can only show total btc amount if we have requested to get the amount for each entry with the -f fields flag
		if fieldsSelected["amount"] {
			fmt.Printf("Total BTC:   %.8f\n", float64(totalAmount) / float64(100000000)) // convert satoshis to BTC (float with 8 decimal places)
		}

		// Can only show script type stats if we have requested to get the script type for each entry with the -f fields flag
		if fieldsSelected["type"] {
			fmt.Println("Script Types:")
			for k, v := range scriptTypeCount {
				fmt.Printf(" %-12s %d\n", k, v) // %-12s = left-justify padding
			}
		}
	}
}

// processUTXOsParallel processes UTXOs using multiple workers
func processUTXOsParallel(db *leveldb.DB, obfuscateKey []byte, fieldsSelected map[string]bool, testnet bool, p2pkaddresses bool, numWorkers int, fields string) (<-chan UTXOResult, int) {
	jobs := make(chan UTXOJob, 10000)
	results := make(chan UTXOResult, 10000)
	
	var wg sync.WaitGroup
	totalUTXOs := 0
	
	// Count total UTXOs first for progress reporting
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		if iter.Key()[0] == 67 { // UTXO entry
			totalUTXOs++
		}
	}
	iter.Release()
	
	if totalUTXOs == 0 {
		close(results)
		return results, 0
	}
	
	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for job := range jobs {
				result := processSingleUTXO(job.Key, job.Value, obfuscateKey, fieldsSelected, testnet, p2pkaddresses, job.Count, fields)
				results <- result
			}
		}(w)
	}
	
	// Send jobs
	go func() {
		iter := db.NewIterator(nil, nil)
		count := 0
		for iter.Next() {
			key := iter.Key()
			if key[0] == 67 { // UTXO entry
				// Copy key/value since they'll be reused in iterator
				keyCopy := make([]byte, len(key))
				valueCopy := make([]byte, len(iter.Value()))
				copy(keyCopy, key)
				copy(valueCopy, iter.Value())
				
				jobs <- UTXOJob{
					Key:   keyCopy,
					Value: valueCopy,
					Count: count,
				}
				count++
			}
		}
		iter.Release()
		close(jobs)
	}()
	
	// Close results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()
	
	return results, totalUTXOs
}

// processSingleUTXO processes a single UTXO entry
func processSingleUTXO(key, value, obfuscateKey []byte, fieldsSelected map[string]bool, testnet bool, p2pkaddresses bool, count int, fields string) UTXOResult {
	output := map[string]string{}
	var amount int64 = 0
	scriptType := "non-standard"
	
	// Key processing
	if fieldsSelected["txid"] {
		txidLE := key[1:33] // little-endian byte order

		// txid - reverse byte order
		txid := make([]byte, 0, 32)
		for i := len(txidLE)-1; i >= 0; i-- {
			txid = append(txid, txidLE[i])
		}
		output["txid"] = hex.EncodeToString(txid)
	}

	if fieldsSelected["vout"] {
		index := key[33:]
		vout := btcleveldb.Varint128Decode(index)
		output["vout"] = strconv.FormatInt(vout, 10)
	}

	// Value processing
	if fieldsSelected["type"] || fieldsSelected["height"] || fieldsSelected["coinbase"] || fieldsSelected["amount"] || fieldsSelected["nsize"] || fieldsSelected["script"] || fieldsSelected["address"] {
		if len(obfuscateKey) > 0 {
			obfuscateKeyExtended := obfuscateKey[1:]
			for i, k := len(obfuscateKeyExtended), 0; len(obfuscateKeyExtended) < len(value); i, k = i+1, k+1 {
				obfuscateKeyExtended = append(obfuscateKeyExtended, obfuscateKeyExtended[k])
			}

			var xor []byte
			for i := range value {
				result := value[i] ^ obfuscateKeyExtended[i]
				xor = append(xor, result)
			}

			offset := 0

			// First Varint
			varint, bytesRead := btcleveldb.Varint128Read(xor, 0)
			offset += bytesRead
			varintDecoded := btcleveldb.Varint128Decode(varint)

			if fieldsSelected["height"] || fieldsSelected["coinbase"] {
				height := varintDecoded >> 1
				output["height"] = strconv.FormatInt(height, 10)
				coinbase := varintDecoded & 1
				output["coinbase"] = strconv.FormatInt(coinbase, 10)
			}

			// Second Varint
			varint, bytesRead = btcleveldb.Varint128Read(xor, offset)
			offset += bytesRead
			varintDecoded = btcleveldb.Varint128Decode(varint)

			if fieldsSelected["amount"] {
				amount = btcleveldb.DecompressValue(varintDecoded)
				output["amount"] = strconv.FormatInt(amount, 10)
			}

			// Third Varint
			varint, bytesRead = btcleveldb.Varint128Read(xor, offset)
			offset += bytesRead
			nsize := btcleveldb.Varint128Decode(varint)
			output["nsize"] = strconv.FormatInt(nsize, 10)

			// Move offset back a byte if script type is 2, 3, 4, or 5
			if nsize > 1 && nsize < 6 {
				offset--
			}
			
			// Get the remaining bytes
			script := xor[offset:]
			
			// Decompress the public keys from P2PK scripts that were uncompressed originally
			if (nsize == 4 || nsize == 5) && (fieldsSelected["script"] || (fieldsSelected["address"] && p2pkaddresses)) {
				script = keys.DecompressPublicKey(script)
			}
			
			if fieldsSelected["script"] {
				output["script"] = hex.EncodeToString(script)
			}

			// Addresses and script type
			if fieldsSelected["address"] || fieldsSelected["type"] {
				var address string
				scriptType = "non-standard"

				switch {
				case nsize == 0:
					if fieldsSelected["address"] {
						if testnet {
							address = keys.Hash160ToAddress(script, []byte{0x6f})
						} else {
							address = keys.Hash160ToAddress(script, []byte{0x00})
						}
					}
					scriptType = "p2pkh"

				case nsize == 1:
					if fieldsSelected["address"] {
						if testnet {
							address = keys.Hash160ToAddress(script, []byte{0xc4})
						} else {
							address = keys.Hash160ToAddress(script, []byte{0x05})
						}
					}
					scriptType = "p2sh"

				case 1 < nsize && nsize < 6:
					scriptType = "p2pk"
					if fieldsSelected["address"] && p2pkaddresses {
						if testnet {
							address = keys.PublicKeyToAddress(script, []byte{0x6f})
						} else {
							address = keys.PublicKeyToAddress(script, []byte{0x00})
						}
					}

				case len(script) > 0 && script[len(script)-1] == 174:
					scriptType = "p2ms"

				case nsize == 28 && script[0] == 0 && script[1] == 20:
					version := script[0]
					program := script[2:]
					var programint []int
					for _, v := range program {
						programint = append(programint, int(v))
					}
					if fieldsSelected["address"] {
						if testnet {
							address, _ = bech32.SegwitAddrEncode("tb", int(version), programint)
						} else {
							address, _ = bech32.SegwitAddrEncode("bc", int(version), programint)
						}
					}
					scriptType = "p2wpkh"

				case nsize == 40 && script[0] == 0 && script[1] == 32:
					version := script[0]
					program := script[2:]
					var programint []int
					for _, v := range program {
						programint = append(programint, int(v))
					}
					if fieldsSelected["address"] {
						if testnet {
							address, _ = bech32.SegwitAddrEncode("tb", int(version), programint)
						} else {
							address, _ = bech32.SegwitAddrEncode("bc", int(version), programint)
						}
					}
					scriptType = "p2wsh"

				case nsize == 40 && script[0] == 0x51 && script[1] == 32:
					version := 1
					program := script[2:]
					var programint []int
					for _, v := range program {
						programint = append(programint, int(v))
					}
					if fieldsSelected["address"] {
						if testnet {
							address, _ = bech32.SegwitAddrEncode("tb", version, programint)
						} else {
							address, _ = bech32.SegwitAddrEncode("bc", version, programint)
						}
					}
					scriptType = "p2tr"

				default:
					scriptType = "non-standard"
				}
				
				output["address"] = address
				output["type"] = scriptType
			}
		}
	}

	// Build CSV line
	output["count"] = strconv.Itoa(count + 1)
	var sb strings.Builder
	sb.Grow(256) // Pre-allocate reasonable size
	
	fieldList := strings.Split(fields, ",")
	for i, v := range fieldList {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(output[v])
	}
	csvline := sb.String()

	return UTXOResult{
		CSVLine:   csvline,
		Count:	 count,
		Amount:	amount,
		ScriptType: scriptType,
		Error:	 nil,
	}
}
