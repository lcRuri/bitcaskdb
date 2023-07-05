package root

import (
	bitcask_go "bitcask-go"
	"bitcask-go/cmd/server"
	"bitcask-go/index"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"log"
	"os"
)

var configFile string
var cmdIndexType, cmdDirPath, cmdPort string
var cmdSyncWrites, cmdMMapAtStartUp *bool
var cmdDataFileMergeRatio *float32
var cmdBytesPerSync *uint
var cmdDataFileSize *int64

var standaloneCmd = &cobra.Command{
	Use:   "standalone",
	Short: "run ruri alone",
	Long:  "ruri's goal is to compromise between performance and storage costs, as an alternative to Redis in some scenarios.",
	Run: func(cmd *cobra.Command, args []string) {
		//配置文件不为空
		if configFile != "" {
			viper.SetConfigFile(configFile)

			err := viper.ReadInConfig()
			if err != nil {
				fmt.Printf("Unable to read configuration file: %s, please check whether the path is correct \n", configFile)
				os.Exit(1)
			}
		} else {
			viper.Set("standalone.port", cmdPort)

			viper.Set("engine.dirPath", cmdDirPath)
			viper.Set("engine.dataFileSize", *cmdDataFileSize)
			viper.Set("engine.syncWrites", *cmdSyncWrites)
			viper.Set("engine.bytesPerSync", *cmdBytesPerSync)
			viper.Set("engine.indexType", cmdIndexType)
			viper.Set("engine.mMapAtStartUp", *cmdMMapAtStartUp)
			viper.Set("engine.dataFileMergeRatio", *cmdDataFileMergeRatio)
		}

		//读取配置
		addr := viper.GetString("standalone.addr")

		dirPath := viper.GetString("engine.dirPath")
		dataFileSize := viper.GetInt64("engine.dataFileSize")
		syncWrites := viper.GetBool("engine.syncWrites")
		bytesPerSync := viper.GetUint("engine.bytesPerSync")
		indexType := viper.GetString("engine.indexType")
		mMapAtStartUp := viper.GetBool("engine.mMapAtStartUp")
		dataFileMergeRatio := viper.GetFloat64("engine.dataFileMergeRatio")

		bcOpt := bitcask_go.Options{
			DirPath:            dirPath,
			DataFileSize:       dataFileSize,
			SyncWrites:         syncWrites,
			BytesPerSync:       bytesPerSync,
			MMapAtStartUp:      mMapAtStartUp,
			DataFileMergeRatio: float32(dataFileMergeRatio),
		}

		switch indexType {
		case "btree":
			bcOpt.IndexType = index.Btree
		case "art":
			bcOpt.IndexType = index.ART
		case "bptree":
			bcOpt.IndexType = index.BPTree
		}

		if addr == "" {
			log.Printf("unable to get addr\n")
			return
		}
		runOpt := &server.RunOptions{
			StandaloneOpt: bcOpt,
			Addr:          addr,
		}

		server.StartEngine(runOpt)
	},
}

func init() {
	standaloneCmd.Flags().StringVarP(&configFile, "cpath", "c", "", "Path of the configuration file in yaml, json and toml format (optional)")
	standaloneCmd.Flags().StringVarP(&cmdPort, "port", "p", ":9736", "Address of the host on the network (For example 192.168.1.151:9736) [default 0.0.0.0:9736]")

	standaloneCmd.Flags().StringVarP(&cmdDirPath, "dpath", "d", "./store", "Directory Path where data logs are stored [default at ./datafile]")
	standaloneCmd.Flags().StringVarP(&cmdIndexType, "itype", "t", "btree", "Type of memory index (bptree/btree/art)")
	cmdDataFileSize = standaloneCmd.Flags().Int64P("size", "", 268435456, "Maximum byte size per datafile (unit: Byte) [default 256MB]")
	cmdSyncWrites = standaloneCmd.Flags().BoolP("sync", "", false, "Whether to enable write synchronization (true/false)")
	cmdBytesPerSync = standaloneCmd.Flags().UintP("bytes", "", 0, "How many bytes are accumulated after they are persisted")
	cmdMMapAtStartUp = standaloneCmd.Flags().BoolP("mmap", "", true, "Whether mmap is enabled")
	cmdDataFileMergeRatio = standaloneCmd.Flags().Float32P("merge", "", 0.5, "The threshold for data file merging")

	AddCommands(standaloneCmd)

}
