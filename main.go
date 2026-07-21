package main

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/singleflight"

	"github.com/protomaps/go-pmtiles/pmtiles"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"

	extractGroup singleflight.Group

	maxZoomRegex = regexp.MustCompile(`^/([^/]+)-(\d+)\.pmtiles$`)
	minZoomRegex = regexp.MustCompile(`^/([^/]+)\+(\d+)-(\d+)-(\d+)\.pmtiles$`)
)

var cli struct {
	Show struct {
		Path       string `arg:""`
		Bucket     string `help:"Remote bucket"`
		Metadata   bool   `help:"Print only the JSON metadata"`
		HeaderJson bool   `help:"Print a JSON representation of part of the header information"`
		Tilejson   bool   `help:"Print the TileJSON"`
		PublicURL  string `help:"Public base URL of tile endpoint for TileJSON e.g. https://example.com/tiles"`
	} `cmd:"" help:"Inspect a local or remote archive"`

	Tile struct {
		Path   string `arg:""`
		Z      int    `arg:""`
		X      int    `arg:""`
		Y      int    `arg:""`
		Bucket string `help:"Remote bucket"`
	} `cmd:"" help:"Fetch one tile from a local or remote archive and output on stdout"`

	Cluster struct {
		Input           string `arg:"" help:"Input archive" type:"existingfile"`
		NoDeduplication bool   `help:"Don't attempt to deduplicate tiles"`
	} `cmd:"" help:"Cluster an unclustered local archive, optimizing the size and layout"`

	Edit struct {
		Input      string `arg:"" help:"Input archive" type:"existingfile"`
		HeaderJson string `help:"Input header JSON file (written by show --header-json)" type:"existingfile"`
		Metadata   string `help:"Input metadata JSON (written by show --metadata)" type:"existingfile"`
	} `cmd:"" help:"Edit JSON metadata or parts of the header"`

	Extract struct {
		Input           string  `arg:"" help:"Input local or remote archive"`
		Output          string  `arg:"" help:"Output archive" type:"path"`
		Bucket          string  `help:"Remote bucket of input archive"`
		Region          string  `help:"local GeoJSON Polygon or MultiPolygon file for area of interest" type:"existingfile"`
		Bbox            string  `help:"bbox area of interest: min_lon,min_lat,max_lon,max_lat" type:"string"`
		Tile            string  `help:"Tile ID of the area of interest: Zoom,X,Y" type:"string"`
		Slice           bool    `help:"Slice output files at --minzooom. --output is a template with {x},{y} and optional {z}"`
		Minzoom         int8    `default:"-1" help:"Minimum zoom level, inclusive"`
		Maxzoom         int8    `default:"-1" help:"Maximum zoom level, inclusive"`
		DownloadThreads int     `default:"4" help:"Number of download threads"`
		DryRun          bool    `help:"Calculate tiles to extract, but don't download them"`
		Overfetch       float32 `default:"0.05" help:"What ratio of extra data to download to minimize # requests; 0.2 is 20%"`
	} `cmd:"" help:"Create an archive from a larger archive for a subset of zoom levels or geographic region"`

	Merge struct {
		Output string   `arg:"" help:"Output archive" type:"path"`
		Input  []string `arg:"" help:"Input archives"`
	} `cmd:"" help:"Merge multiple archives into a single archive" hidden:""`

	Convert struct {
		Input           string `arg:"" help:"Input archive" type:"existingfile"`
		Output          string `arg:"" help:"Output archive" type:"path"`
		Force           bool   `help:"Force removal"`
		NoDeduplication bool   `help:"Don't attempt to deduplicate tiles"`
		Tmpdir          string `help:"An optional path to a folder for temporary files" type:"existingdir"`
	} `cmd:"" help:"Convert an MBTiles database to PMTiles"`

	Verify struct {
		Input string `arg:"" help:"Input archive" type:"existingfile"`
	} `cmd:"" help:"Verify the correctness of an archive structure, without verifying individual tile contents"`

	Makesync struct {
		Input       string `arg:"" type:"existingfile"`
		BlockSizeKb int    `default:"20" help:"The approximate block size, in kilobytes; 0 means 1 tile = 1 block"`
	} `cmd:"" help:"" hidden:""`

	Sync struct {
		Existing         string `arg:"" type:"existingfile"`
		New              string `arg:"" help:"Local or remote archive, with .sync sidecar file"`
		DryRun           bool   `help:"Calculate new parts to download, but don't download them"`
		RangesPerRequest int    `default:"100" help:"Number of ranges in a single HTTP request (limit depends on server)"`
	} `cmd:"" help:"Sync a local file with a remote one by only downloading changed parts" hidden:""`

	Serve struct {
		Path      string `arg:"" help:"Local path or bucket prefix"`
		Interface string `default:"0.0.0.0"`
		Port      int    `default:"8080"`
		AdminPort int    `default:"-1"`
		Cors      string `help:"Comma-separated list of of allowed HTTP CORS origins"`
		CacheSize int    `default:"64" help:"Size of cache in megabytes"`
		Bucket    string `help:"Remote bucket"`
		PublicURL string `help:"Public base URL of tile endpoint for TileJSON e.g. https://example.com/tiles/"`
	} `cmd:"" help:"Run an HTTP proxy server for Z/X/Y tiles"`

	ServeExtract struct {
		SourceDir string `arg:"" help:"Directory containing source PMTiles archives" type:"existingdir"`
		CacheDir  string `arg:"" help:"Directory to store extracted PMTiles caches" type:"existingdir"`
		Interface string `default:"0.0.0.0"`
		Port      int    `default:"8080"`
		Cors      string `help:"Comma-separated list of allowed HTTP CORS origins"`
	} `cmd:"" help:"Run an HTTP server that performs on-demand slicing of PMTiles sources"`

	Upload struct {
		InputPmtiles   string `arg:"" type:"existingfile" help:"The local PMTiles file"`
		RemotePmtiles  string `arg:""  help:"The name for the remote PMTiles source"`
		MaxConcurrency int    `default:"2" help:"# of upload threads"`
		Bucket         string `required:"" help:"Bucket to upload to"`
	} `cmd:"" help:"Upload a local archive to remote storage"`

	Version struct {
	} `cmd:"" help:"Show the program version"`
}

func main() {
	if len(os.Args) < 2 {
		os.Args = append(os.Args, "--help")
	}

	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.Lshortfile)
	ctx := kong.Parse(&cli)

	switch ctx.Command() {
	case "show <path>":
		err := pmtiles.Show(logger, os.Stdout, cli.Show.Bucket, cli.Show.Path, cli.Show.HeaderJson, cli.Show.Metadata, cli.Show.Tilejson, cli.Show.PublicURL, false, 0, 0, 0)
		if err != nil {
			logger.Fatalf("Failed to show archive, %v", err)
		}
	case "tile <path> <z> <x> <y>":
		err := pmtiles.Show(logger, os.Stdout, cli.Tile.Bucket, cli.Tile.Path, false, false, false, "", true, cli.Tile.Z, cli.Tile.X, cli.Tile.Y)
		if err != nil {
			logger.Fatalf("Failed to show tile, %v", err)
		}
	case "serve <path>":
		server, err := pmtiles.NewServer(cli.Serve.Bucket, cli.Serve.Path, logger, cli.Serve.CacheSize, cli.Serve.PublicURL)

		if err != nil {
			logger.Fatalf("Failed to create new server, %v", err)
		}

		pmtiles.SetBuildInfo(version, commit, date)
		server.Start()

		mux := http.NewServeMux()

		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			statusCode := server.ServeHTTP(w, r)
			logger.Printf("served %d %s in %s", statusCode, url.PathEscape(r.URL.Path), time.Since(start))
		})

		logger.Printf("Serving %s %s on port %d and interface %s with Access-Control-Allow-Origin: %s\n", cli.Serve.Bucket, cli.Serve.Path, cli.Serve.Port, cli.Serve.Interface, cli.Serve.Cors)
		if cli.Serve.AdminPort > 0 {
			go func() {
				adminPort := strconv.Itoa(cli.Serve.AdminPort)
				logger.Printf("Serving /metrics on port %s and interface %s\n", adminPort, cli.Serve.Interface)
				adminMux := http.NewServeMux()
				adminMux.Handle("/metrics", promhttp.Handler())
				logger.Fatal(startHTTPServer(cli.Serve.Interface+":"+adminPort, adminMux))
			}()
		}

		if cli.Serve.Cors != "" {
			muxWithCors := pmtiles.NewCors(cli.Serve.Cors).Handler(mux)
			logger.Fatal(startHTTPServer(cli.Serve.Interface+":"+strconv.Itoa(cli.Serve.Port), muxWithCors))
		} else {
			logger.Fatal(startHTTPServer(cli.Serve.Interface+":"+strconv.Itoa(cli.Serve.Port), mux))
		}
	case "serve-extract <source-dir> <cache-dir>":
		pmtiles.SetBuildInfo(version, commit, date)

		mux := http.NewServeMux()
		mux.HandleFunc("/", ExtractServeHandler(cli.ServeExtract.CacheDir, cli.ServeExtract.SourceDir))

		logger.Printf("Serving extracts from %s to %s on port %d and interface %s with Access-Control-Allow-Origin: %s\n", cli.ServeExtract.SourceDir, cli.ServeExtract.CacheDir, cli.ServeExtract.Port, cli.ServeExtract.Interface, cli.ServeExtract.Cors)
		
		if cli.ServeExtract.Cors != "" {
			muxWithCors := pmtiles.NewCors(cli.ServeExtract.Cors).Handler(mux)
			logger.Fatal(startHTTPServer(cli.ServeExtract.Interface+":"+strconv.Itoa(cli.ServeExtract.Port), muxWithCors))
		} else {
			logger.Fatal(startHTTPServer(cli.ServeExtract.Interface+":"+strconv.Itoa(cli.ServeExtract.Port), mux))
		}
	case "extract <input> <output>":
		if cli.Extract.Slice {
			if cli.Extract.Region != "" || cli.Extract.Bbox != "" || cli.Extract.Tile != "" {
				logger.Fatalf("Only one of slice, region, bbox, and tile can be specified")
			}
			numtiles := int(math.Pow(2, float64(cli.Extract.Minzoom)))
			bar := progressbar.NewOptions64(
				int64(numtiles)*int64(numtiles),
				progressbar.OptionSetDescription("Slicing"),
				progressbar.OptionSetItsString("files"),
				progressbar.OptionShowCount(),
				progressbar.OptionShowIts(),
				progressbar.OptionShowElapsedTimeOnFinish(),
			)
			outputZ := strings.ReplaceAll(cli.Extract.Output, "{z}", strconv.Itoa(int(cli.Extract.Minzoom)))
			for x := 0; x < numtiles; x++ {
				outputZX := strings.ReplaceAll(outputZ, "{x}", strconv.Itoa(x))
				for y := 0; y < numtiles; y++ {
					outputZXY := strings.ReplaceAll(outputZX, "{y}", strconv.Itoa(y))
					dir := filepath.Dir(outputZXY)
					err := os.MkdirAll(dir, 0755)
					if err != nil {
						logger.Fatalf("Error creating directory '%s': %v\n", dir, err)
					}
					tile := fmt.Sprintf("%d,%d,%d", cli.Extract.Minzoom, x, y)
					err = pmtiles.Extract(logger, cli.Extract.Bucket, cli.Extract.Input, cli.Extract.Minzoom, cli.Extract.Maxzoom, "", "", tile, outputZXY, cli.Extract.DownloadThreads, cli.Extract.Overfetch, cli.Extract.DryRun, true)
					bar.Add(1)
					if err != nil {
						logger.Fatalf("Failed to extract, %v", err)
					}
				}
			}
			fmt.Println()
		} else {
			err := pmtiles.Extract(logger, cli.Extract.Bucket, cli.Extract.Input, cli.Extract.Minzoom, cli.Extract.Maxzoom, cli.Extract.Region, cli.Extract.Bbox, cli.Extract.Tile, cli.Extract.Output, cli.Extract.DownloadThreads, cli.Extract.Overfetch, cli.Extract.DryRun, false)
			if err != nil {
				logger.Fatalf("Failed to extract, %v", err)
			}
		}
	case "cluster <input>":
		err := pmtiles.Cluster(logger, cli.Cluster.Input, !cli.Cluster.NoDeduplication)
		if err != nil {
			logger.Fatalf("Failed to cluster, %v", err)
		}
	case "convert <input> <output>":
		path := cli.Convert.Input
		output := cli.Convert.Output

		var tmpfile *os.File

		if cli.Convert.Tmpdir == "" {
			var err error
			tmpfile, err = os.CreateTemp("", "pmtiles")

			if err != nil {
				logger.Fatalf("Failed to create temp file, %v", err)
			}
		} else {
			absTemproot, err := filepath.Abs(cli.Convert.Tmpdir)

			if err != nil {
				logger.Fatalf("Failed to derive absolute path for %s, %v", cli.Convert.Tmpdir, err)
			}

			tmpfile, err = os.CreateTemp(absTemproot, "pmtiles")

			if err != nil {
				logger.Fatalf("Failed to create temp file, %v", err)
			}
		}

		defer os.Remove(tmpfile.Name())
		err := pmtiles.Convert(logger, path, output, !cli.Convert.NoDeduplication, tmpfile)

		if err != nil {
			logger.Fatalf("Failed to convert %s, %v", path, err)
		}
	case "upload <input-pmtiles> <remote-pmtiles>":
		err := pmtiles.Upload(logger, cli.Upload.InputPmtiles, cli.Upload.Bucket, cli.Upload.RemotePmtiles, cli.Upload.MaxConcurrency)

		if err != nil {
			logger.Fatalf("Failed to upload file, %v", err)
		}
	case "verify <input>":
		err := pmtiles.Verify(logger, cli.Verify.Input)
		if err != nil {
			logger.Fatalf("Failed to verify archive, %v", err)
		}
	case "edit <input>":
		err := pmtiles.Edit(logger, cli.Edit.Input, cli.Edit.HeaderJson, cli.Edit.Metadata)
		if err != nil {
			logger.Fatalf("Failed to edit archive, %v", err)
		}
	case "makesync <input>":
		err := pmtiles.Makesync(logger, version, cli.Makesync.Input, cli.Makesync.BlockSizeKb)
		if err != nil {
			logger.Fatalf("Failed to makesync archive, %v", err)
		}
	case "sync <existing> <new>":
		err := pmtiles.Sync(logger, cli.Sync.Existing, cli.Sync.New, cli.Sync.DryRun)
		if err != nil {
			logger.Fatalf("Failed to sync archive, %v", err)
		}
	case "version":
		fmt.Printf("pmtiles %s, commit %s, built at %s\n", version, commit, date)
	default:
		panic(ctx.Command())
	}
}

func startHTTPServer(addr string, handler http.Handler) error {
	server := &http.Server{
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
		Addr:              addr,
		Handler:           handler,
	}
	return server.ListenAndServe()
}

func ExtractServeHandler(baseCacheDir, sourceDir string) http.HandlerFunc {
	// os.SameFile compares underlying inodes and device IDs, guaranteeing detection of overlap
	// even when different host volume aliases are mounted to the container. This prevents 
	// accidental overwrite of source files.
	sourceStat, errSrc := os.Stat(sourceDir)
	cacheStat, errCache := os.Stat(baseCacheDir)
	if errSrc == nil && errCache == nil && os.SameFile(sourceStat, cacheStat) {
		log.Fatalf("FATAL: sourceDir and baseCacheDir point to the same underlying directory. This must be separated to prevent data destruction.")
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var archiveName string
		var minZoom, maxZoom int8
		var tileStr string
		var cachePath string

		if m := maxZoomRegex.FindStringSubmatch(r.URL.Path); m != nil {
			archiveName = filepath.Base(m[1])
			if archiveName == "." || archiveName == "/" || archiveName == "\\" {
				http.Error(w, "Invalid archive name", http.StatusBadRequest)
				return
			}
			
			parsedMax, err := strconv.ParseInt(m[2], 10, 8)
			if err != nil || parsedMax < 0 || parsedMax > 24 {
				http.Error(w, "Invalid maximum zoom level", http.StatusBadRequest)
				return
			}
			
			maxZoom = int8(parsedMax)
			minZoom = 0
			tileStr = ""
			cachePath = fmt.Sprintf("%s-%d.pmtiles", archiveName, maxZoom)

		} else if m := minZoomRegex.FindStringSubmatch(r.URL.Path); m != nil {
			archiveName = filepath.Base(m[1])
			if archiveName == "." || archiveName == "/" || archiveName == "\\" {
				http.Error(w, "Invalid archive name", http.StatusBadRequest)
				return
			}

			parsedZ, err := strconv.ParseInt(m[2], 10, 8)
			if err != nil || parsedZ < 0 || parsedZ > 24 {
				http.Error(w, "Invalid zoom level", http.StatusBadRequest)
				return
			}
			z := int8(parsedZ)

			x, _ := strconv.ParseInt(m[3], 10, 64)
			y, _ := strconv.ParseInt(m[4], 10, 64)

			limit := int64(1 << z)
			if x < 0 || x >= limit || y < 0 || y >= limit {
				http.Error(w, "Coordinates out of bounds for given zoom level", http.StatusBadRequest)
				return
			}

			minZoom = z
			maxZoom = z
			tileStr = fmt.Sprintf("%d/%d/%d", z, x, y)
			cachePath = fmt.Sprintf("%s+%d-%d-%d.pmtiles", archiveName, z, x, y)

		} else {
			http.NotFound(w, r)
			return
		}

		sourceFile := filepath.Join(sourceDir, archiveName+".pmtiles")
		cacheFile := filepath.Join(baseCacheDir, cachePath)
		
		serveExtracted(w, r, sourceFile, baseCacheDir, cacheFile, minZoom, maxZoom, tileStr)
	}
}

func serveExtracted(w http.ResponseWriter, r *http.Request, sourceFile, baseCacheDir, cacheFile string, minZoom, maxZoom int8, tileStr string) {
	for {
		if f, err := os.Open(cacheFile); err == nil {
			stat, err := f.Stat()
			if err == nil {
				defer f.Close()
				http.ServeContent(w, r, stat.Name(), stat.ModTime(), f)
				return
			}
			f.Close()
		}

		// singleflight prevents cache stampedes for the same extraction parameters
		_, err, _ := extractGroup.Do(cacheFile, func() (interface{}, error) {
			if _, err := os.Stat(cacheFile); err == nil {
				return nil, nil
			}

			tmpFile, err := os.CreateTemp(baseCacheDir, "pmtiles-extract-*.tmp")
			if err != nil {
				return nil, fmt.Errorf("failed to create temporary file: %w", err)
			}
			tempName := tmpFile.Name()
			
			tmpFile.Close() 
			defer os.Remove(tempName)

			err = pmtiles.Extract(
				nil,
				sourceFile,
				"",
				minZoom,
				maxZoom,
				"",
				"",
				tileStr,
				tempName,
				4,
				0.2,
				false,
				true,
			)
			if err != nil {
				return nil, fmt.Errorf("extraction failed: %w", err)
			}

			targetDir := filepath.Dir(cacheFile)

			if err := os.MkdirAll(targetDir, 0755); err != nil {
				return nil, fmt.Errorf("failed to create target cache directories: %w", err)
			}

			if err := os.Rename(tempName, cacheFile); err != nil {
				return nil, fmt.Errorf("failed to atomically finalize cache file: %w", err)
			}

			return nil, nil
		})

		if err != nil {
			// Defends against TOCTOU race condition if source file is deleted/moved prior to extraction
			if errors.Is(err, os.ErrNotExist) {
				http.Error(w, "Source archive not found", http.StatusNotFound)
				return
			}
			
			log.Printf("Error serving extract for %s: %v", cacheFile, err)
			http.Error(w, "Failed to extract tile archive", http.StatusInternalServerError)
			return
		}
	}
}
